import requests
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2
from constants import API_VEHICLE_POSITIONS, API_SERVICE_ALERTS, API_TRIP_UPDATES
from datetime import datetime, timezone
import logging
import pandas as pd
from utils import validate_vehicle_frame, match_trip_updates_to_static
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

headers = {
    'accept': 'application/x-protobuf',
    'apiKey': "l7847ecf8f41dc4800a5c94cf684c0b37f" # Environment variables
}

while True:

    # Ping API
    response = requests.get(url=API_VEHICLE_POSITIONS, headers=headers, timeout=15)

    # Read response into GTFS Feed
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    header = feed.header
    version = header.gtfs_realtime_version
    mode = header.incrementality  # 0 = FULL_DATASET, 1 = DIFFERENTIAL
    built_at = datetime.fromtimestamp(header.timestamp, tz=timezone.utc)

    logging.info("Feed version %s, mode=%s, built_at=%s (UTC)", version, mode, built_at.isoformat())

    if mode != gtfs_realtime_pb2.FeedHeader.FULL_DATASET:
        logging.warning("Feed is DIFFERENTIAL — entities must be applied as deltas, not a replace")

    # Staleness guard: reject snapshots older than 90 seconds
    age_seconds = datetime.now(tz=timezone.utc).timestamp() - header.timestamp
    if age_seconds > 90:
        logging.warning("Feed snapshot is %.0f s old — producer may be stalled", age_seconds)


    # Extract rowsd
    vehicle_rows = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vp = entity.vehicle
            vehicle_rows.append(
                {
                    "entity_id": entity.id,
                    "vehicle_id": vp.vehicle.id or None,
                    "trip_id": vp.trip.trip_id or None,
                    "route_id": vp.trip.route_id or None,
                    "latitude": vp.position.latitude if vp.HasField("position") else None,
                    "longitude": vp.position.longitude if vp.HasField("position") else None,
                    "bearing": vp.position.bearing if vp.HasField("position") else None,
                    "current_stop_sequence": vp.current_stop_sequence,
                    "stop_id": vp.stop_id or None,
                    "timestamp": vp.timestamp or None,
                }
            )
            # trip = entity.vehicle.trip
            # position = entity.vehicle.position
            # route_id = trip.route_id
            # lat = position.latitude
            # long = position.longitude
            # print( "Route ID:", route_id, "Lat:", lat, "Long:", long)

    # Flatten to dataframe
    VEHICLE_DTYPES = {
        "entity_id": "string",
        "vehicle_id": "string",
        "trip_id": "string",
        "route_id": "string",
        "latitude": "float64",
        "longitude": "float64",
        "bearing": "float64",
        "current_stop_sequence": "Int64",
        "stop_id": "string",
        "timestamp": "Int64",
    }

    vehicle_positions = pd.DataFrame(vehicle_rows).astype(VEHICLE_DTYPES)

    # Convert POSIX seconds to a UTC-aware datetime column for downstream joins
    vehicle_positions["observed_at"] = pd.to_datetime(
        vehicle_positions["timestamp"], unit="s", utc=True
    )

    report = validate_vehicle_frame(vehicle_positions, feed.header.timestamp)
    logging.info("Validation report: %s", report)

    logging.info("Built vehicle_positions frame: %d rows", len(vehicle_positions))
    print(vehicle_positions.head().to_string(index=False))
    time.sleep(15)
