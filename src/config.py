import os
from dotenv import load_dotenv

load_dotenv()

APIS = {
    "STM": {
        "key": os.environ.get("STM_API_KEY"),
        "vehicle_positions": "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions",
        "trip_updates": "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates",
        "service_alerts": "https://api.stm.info/pub/od/i3/v2/messages/etatservice"
    },
    "MBTA": {
        "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.pb"
    }
}

if not APIS['STM']['key']:
    raise RuntimeError("Missing STM_API_KEY environment variable. Did you load .env?")

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

missing_db_vars = [k for k in ("dbname", "user", "password") if not DB_CONFIG[k]]
if missing_db_vars:
    raise RuntimeError(f"Missing Postgres environment variables in .env: {missing_db_vars}")