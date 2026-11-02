import requests
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2

API_VEHICLE_POSITIONS = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions"
headers = {
    'accept': 'application/x-protobuf',
    'apiKey': API_KEY # Environment variables
}

response = requests.get(url=API_VEHICLE_POSITIONS, headers=headers)

# Read response into GTFS Feed
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)
print(feed.entity)
