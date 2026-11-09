import os
from dotenv import load_dotenv

API_TRIP_UPDATES = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
API_VEHICLE_POSITIONS = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions"
API_SERVICE_ALERTS = "https://api.stm.info/pub/od/i3/v2/messages/etatservice"

load_dotenv()
API_KEY = os.environ.get("STM_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing STM_API_KEY environment variable. Did you load .env?")