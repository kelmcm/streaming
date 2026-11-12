import asyncio
from fastapi import FastAPI, WebSocket
from confluent_kafka import Consumer
import websockets

app = FastAPI()

# TODO: Convert to Server Sent Events
@app.websocket("/ws/vehicle_positions")
async def vehicle_stream(websocket: WebSocket):
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'localhost:9092', # port the kafka topic is hosted on

        # Fixed properties
        'group.id':          'gtfs-vehicle-positions',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    consumer.subscribe(["vehicle-positions"])

    try:
        while True:
            # Poll Kafka without blocking the async event loop
            msg = await asyncio.to_thread(consumer.poll, 0.5)

            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
                continue
            else:
                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(),
                    key=msg.key().decode('utf-8'),
                    value=msg.value().decode('utf-8'))
                )

                # Forward the JSON message from Kafka directly over the WebSocket
                await websocket.send(msg.value().decode("utf-8"))
    except  websockets.ConnectionClosed:
        print("Client disconnected")
        pass
    finally:
        consumer.close()

async def main():
    async with websockets.serve(vehicle_stream, "localhost", 8000):
        print("WebSocket server running at ws://localhost:8000")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
