import requests
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2

API_TRIP_UPDATES = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
API_VEHICLE_POSITIONS = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions"
API_SERVICE_ALERTS = "https://api.stm.info/pub/od/i3/v2/messages/etatservice"

headers = {
    'accept': 'application/x-protobuf',
    'apiKey': "l7a79d49af24bb46bb992bde46d158f7ce" # Environment variables
}

response = requests.get(url=API_VEHICLE_POSITIONS, headers=headers)

# Read response into GTFS Feed
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)
print(feed.entity)
