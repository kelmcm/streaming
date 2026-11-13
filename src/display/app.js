const vehicles = new Map(); // agency_id:entity_id -> latest vehicle JSON
const markers = new Map();  // agency_id:entity_id -> maplibregl.Marker
const trails = new Map();   // agency_id:entity_id -> { trip_id, coords: [[lng, lat], ...] }

let mapLoaded = false;
let updateScheduled = false;

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

  mapLoaded = true;
  scheduleTrailUpdate();
});

function scheduleTrailUpdate() {
  if (!mapLoaded || updateScheduled) return;
  updateScheduled = true;
  requestAnimationFrame(() => {
    updateScheduled = false;
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

function createDotElement() {
  const el = document.createElement("div");
  el.style.width = "10px";
  el.style.height = "10px";
  el.style.borderRadius = "50%";
  el.style.backgroundColor = "#78b7da";
  el.style.border = "1.5px solid #ffffff";
  el.style.boxShadow = "0 0 3px rgba(0, 0, 0, 0.4)";
  el.style.cursor = "pointer";
  return el;
}

const vehiclePositionsSource = new EventSource('http://localhost:8000/stream/vehicle_positions');

vehiclePositionsSource.onmessage = (event) => {
  const vehicle = JSON.parse(event.data);
  const key = `${vehicle.agency_id}:${vehicle.entity_id}`;
  vehicles.set(key, vehicle); // overwrites previous position for this bus

  if (vehicle.latitude != null && vehicle.longitude != null) {
    // Append coordinate to this vehicle's trail (reset if it started a new trip)
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

    const popupText = `${vehicle.agency_id} | Route ${vehicle.route_id} | Direction: ${vehicle.direction_id} | Time: ${new Date(vehicle.timestamp * 1000)}`;

    if (markers.has(key)) {
      const marker = markers.get(key);
      marker.setLngLat([vehicle.longitude, vehicle.latitude]);
      marker.getPopup().setText(popupText);
    } else {
      const el = createDotElement();
      const popup = new maplibregl.Popup({
        closeButton: false,
        closeOnClick: false,
        offset: 8,
      }).setText(popupText);

      const marker = new maplibregl.Marker({ element: el })
        .setLngLat([vehicle.longitude, vehicle.latitude])
        .setPopup(popup)
        .addTo(map);

      el.addEventListener("mouseenter", () => {
        if (!popup.isOpen()) marker.togglePopup();
      });
      el.addEventListener("mouseleave", () => {
        if (popup.isOpen()) marker.togglePopup();
      });

      markers.set(key, marker);
    }
  }
};

vehiclePositionsSource.onerror = (err) => {
  console.error("EventSource failed:", err);
};

// vehiclePositionsSource.close()
