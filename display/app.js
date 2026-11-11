const out = document.getElementById("output");
const vehicles = new Map(); // entity_id -> latest vehicle JSON
const markers = new Map();  // entity_id -> maplibregl.Marker

const state = {
  lng: -73.5673, // Montréal
  lat: 45.5017,
  cameraBearing: 0,
  cameraPitch: 0,
};

const map = new maplibregl.Map({
  container: "map",
  style: "https://tiles.openfreemap.org/styles/liberty",
  center: [state.lng, state.lat],
  zoom: 12,
  pitch: state.cameraPitch,
  bearing: state.cameraBearing,
  maxPitch: 85,
  antialias: true,
});

const ws = new WebSocket("ws://localhost:8000");

ws.onmessage = (event) => {
  const vehicle = JSON.parse(event.data);
  vehicles.set(vehicle.entity_id, vehicle); // overwrites previous position for this bus

  if (vehicle.latitude != null && vehicle.longitude != null) {
    if (markers.has(vehicle.entity_id)) {
      markers
        .get(vehicle.entity_id)
        .setLngLat([vehicle.longitude, vehicle.latitude]);
    } else {
      const marker = new maplibregl.Marker({ color: "#007cbf" })
        .setLngLat([vehicle.longitude, vehicle.latitude])
        .setPopup(
          new maplibregl.Popup().setText(
            `Route ${vehicle.route_id} | Vehicle ${vehicle.vehicle_id}`
          )
        )
        .addTo(map);
      markers.set(vehicle.entity_id, marker);
    }
  }

  out.textContent =
    `Tracking ${vehicles.size} vehicles:\n\n` +
    Array.from(vehicles.values())
      .sort((a, b) => Number(a.route_id) - Number(b.route_id))
      .map(
        (v) =>
          `Route: ${v.route_id}, Vehicle: ${v.vehicle_id} | Lat: ${v.latitude}, Lon: ${v.longitude}`
      )
      .join("\n");
};
