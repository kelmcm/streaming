docker start kafka

uv run python src/gtfs_realtime/poller.py

uv run src/gtfs_realtime/consumer.py

# Connect to websocket via terminal
uv run python -m websockets ws://localhost:8000

docker stop kafka