const vehicles = new Map();   // agency_id:entity_id -> latest vehicle JSON
const positions = new Map();  // agency_id:entity_id -> current displayed [lng, lat]
const trails = new Map();     // agency_id:entity_id -> { trip_id, coords: [[lng, lat], ...] }
const animations = new Map(); // agency_id:entity_id -> { path, cumDist, totalDist, startTime, durationMs }

let mapLoaded = false;
let trailUpdateScheduled = false;
let hoveredKey = null;

const state = {
  lng: -73.5673, // Montréal
  lat: 45.5017,
  cameraBearing: 0,
  cameraPitch: 0,
};

const map = new maplibregl.Map({
  container: "map",
  style: "https://tiles.openfreemap.org/styles/positron",
  center: [state.lng, state.lat],
  zoom: 12,
  pitch: state.cameraPitch,
  bearing: state.cameraBearing,
  maxPitch: 85,
  antialias: true,
});

const popup = new maplibregl.Popup({
  closeButton: false,
  closeOnClick: false,
  offset: 8,
});

function formatPopupText(vehicle) {
  const speedText = vehicle.est_speed_kmh != null ? `${vehicle.est_speed_kmh} km/h` : "N/A";
  const errText = vehicle.pred_error_m != null ? `${vehicle.pred_error_m}m` : "N/A";
  return `${vehicle.agency_id} | Route ${vehicle.route_id} | Speed: ${speedText} | Err: ${errText} | ${new Date(vehicle.timestamp * 1000).toLocaleTimeString()}`;
}

map.on("load", () => {
  map.addSource("vehicle-trails", {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
  });

  map.addLayer({
    id: "vehicle-trails-layer",
    type: "line",
    source: "vehicle-trails",
    layout: {
      "line-join": "round",
      "line-cap": "round",
    },
    paint: {
      "line-color": "#78b7da",
      "line-width": 2,
      "line-opacity": 0.65,
    },
  });

  map.addSource("vehicles", {
    type: "geojson",
    data: { type: "FeatureCollection", features: [] },
  });

  map.addLayer({
    id: "vehicles-layer",
    type: "circle",
    source: "vehicles",
    paint: {
      "circle-radius": 5,
      "circle-color": "#78b7da",
      "circle-stroke-width": 1.5,
      "circle-stroke-color": "#ffffff",
    },
  });

  map.on("mouseenter", "vehicles-layer", (e) => {
    map.getCanvas().style.cursor = "pointer";
    const feature = e.features[0];
    hoveredKey = feature.properties.key;
    const vehicle = vehicles.get(hoveredKey);
    if (vehicle) {
      popup
        .setLngLat(feature.geometry.coordinates)
        .setText(formatPopupText(vehicle))
        .addTo(map);
    }
  });

  map.on("mousemove", "vehicles-layer", (e) => {
    const feature = e.features[0];
    hoveredKey = feature.properties.key;
    const vehicle = vehicles.get(hoveredKey);
    if (vehicle) {
      popup
        .setLngLat(feature.geometry.coordinates)
        .setText(formatPopupText(vehicle));
    }
  });

  map.on("mouseleave", "vehicles-layer", () => {
    map.getCanvas().style.cursor = "";
    hoveredKey = null;
    popup.remove();
  });

  mapLoaded = true;
  scheduleTrailUpdate();
});

function scheduleTrailUpdate() {
  if (!mapLoaded || trailUpdateScheduled) return;
  trailUpdateScheduled = true;
  requestAnimationFrame(() => {
    trailUpdateScheduled = false;
    const features = [];
    for (const [key, trail] of trails.entries()) {
      if (trail.coords.length >= 2) {
        features.push({
          type: "Feature",
          properties: { key },
          geometry: {
            type: "LineString",
            coordinates: trail.coords,
          },
        });
      }
    }
    map.getSource("vehicle-trails").setData({
      type: "FeatureCollection",
      features,
    });
  });
}

function buildAnimation(path, durationMs = 25000) {
  const cumDist = [0];
  let totalDist = 0;
  for (let i = 1; i < path.length; i++) {
    const dx = path[i][0] - path[i - 1][0];
    const dy = path[i][1] - path[i - 1][1];
    totalDist += Math.hypot(dx, dy);
    cumDist.push(totalDist);
  }
  return { path, cumDist, totalDist, startTime: performance.now(), durationMs };
}

function interpolateAlongPath(anim, t) {
  const targetDist = t * anim.totalDist;
  for (let i = 1; i < anim.cumDist.length; i++) {
    if (anim.cumDist[i] >= targetDist) {
      const segLen = anim.cumDist[i] - anim.cumDist[i - 1];
      const segT = segLen > 0 ? (targetDist - anim.cumDist[i - 1]) / segLen : 0;
      const [x1, y1] = anim.path[i - 1];
      const [x2, y2] = anim.path[i];
      return [x1 + (x2 - x1) * segT, y1 + (y2 - y1) * segT];
    }
  }
  return anim.path[anim.path.length - 1];
}

function animateVehicles(now) {
  for (const [key, anim] of animations.entries()) {
    if (anim.totalDist === 0) continue;
    const t = Math.min((now - anim.startTime) / anim.durationMs, 1.0);
    positions.set(key, interpolateAlongPath(anim, t));
  }

  if (mapLoaded && positions.size > 0) {
    const features = [];
    for (const [key, coords] of positions.entries()) {
      features.push({
        type: "Feature",
        properties: { key },
        geometry: {
          type: "Point",
          coordinates: coords,
        },
      });
    }
    map.getSource("vehicles").setData({
      type: "FeatureCollection",
      features,
    });

    if (hoveredKey && positions.has(hoveredKey)) {
      popup.setLngLat(positions.get(hoveredKey));
    }
  }

  requestAnimationFrame(animateVehicles);
}
requestAnimationFrame(animateVehicles);

const vehiclePositionsSource = new EventSource("http://localhost:8000/stream/vehicle_positions");

vehiclePositionsSource.onmessage = (event) => {
  const vehicle = JSON.parse(event.data);
  const key = `${vehicle.agency_id}:${vehicle.entity_id}`;
  const prevVehicle = vehicles.get(key);
  vehicles.set(key, vehicle);

  if (vehicle.latitude != null && vehicle.longitude != null) {
    // Append raw GPS coordinate to this vehicle's trail (reset if it started a new trip)
    let trail = trails.get(key);
    if (!trail || trail.trip_id !== vehicle.trip_id) {
      trail = { trip_id: vehicle.trip_id, coords: [] };
      trails.set(key, trail);
    }
    const lastCoord = trail.coords[trail.coords.length - 1];
    if (!lastCoord || lastCoord[0] !== vehicle.longitude || lastCoord[1] !== vehicle.latitude) {
      trail.coords.push([vehicle.longitude, vehicle.latitude]);
      scheduleTrailUpdate();
    }

    const durationMs = (vehicle.horizon_sec ?? 25) * 1000;
    const hasPredictedPath = Array.isArray(vehicle.predicted_path) && vehicle.predicted_path.length >= 2;

    if (hasPredictedPath) {
      const curPos = positions.get(key);
      const sameTrip = prevVehicle && prevVehicle.trip_id === vehicle.trip_id;
      const distFromStart = curPos
        ? Math.hypot(curPos[0] - vehicle.predicted_path[0][0], curPos[1] - vehicle.predicted_path[0][1])
        : Infinity;

      const path = sameTrip && distFromStart < 0.005
        ? [curPos, ...vehicle.predicted_path]
        : vehicle.predicted_path;

      if (!curPos) {
        positions.set(key, vehicle.predicted_path[0]);
      }
      animations.set(key, buildAnimation(path, durationMs));
    } else {
      animations.delete(key);
      positions.set(key, [vehicle.longitude, vehicle.latitude]);
    }

    if (hoveredKey === key) {
      popup.setText(formatPopupText(vehicle));
    }
  }
};

vehiclePositionsSource.onerror = (err) => {
  console.error("EventSource failed:", err);
};
