import asyncio
import os
import uuid
from confluent_kafka import Consumer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.sse import EventSourceResponse, ServerSentEvent
import logging

import json
from gtfs_realtime.vehicle_predictor import VehiclePredictor

logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

predictor = VehiclePredictor()

@app.get("/stream/vehicle_positions", response_class=EventSourceResponse)
async def stream_vehicle_positions():
    config = {
        'bootstrap.servers':  os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'group.id':           f'gtfs-vehicle-positions-{uuid.uuid4()}',
        'auto.offset.reset':  'latest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(config)
    consumer.subscribe(["vehicle-positions"])

    try:
        while True:
            # Grab up to 500 messages at once without blocking the event loop
            msgs = await asyncio.to_thread(consumer.consume, 500, 0.2)

            if not msgs:
                continue

            records = []
            for msg in msgs:
                if msg.error():
                    logger.error(f"ERROR: {msg.error()}")
                    continue
                records.append(json.loads(msg.value().decode("utf-8")))

            if records:
                enriched = await asyncio.to_thread(predictor.enrich_batch, records)
                for record in enriched:
                    yield ServerSentEvent(raw_data=json.dumps(record))


                    # if msg is None:
            #     # Initial message consumption may take up to `session.timeout.ms` for the consumer group to rebalance and start consuming
            #     logger.info("Waiting...")
            # elif msg.error():
            #     logger.error("ERROR: %s".format(msg.error()))
            #     continue
            # else:
            #     # Extract the (optional) key and value, and print.
            #     logger.info("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
            #         topic=msg.topic(),
            #         key=msg.key().decode('utf-8'),
            #         value=msg.value().decode('utf-8'))
            #     )
            #
            #     yield ServerSentEvent(raw_data=msg.value().decode("utf-8"))
    finally:
        consumer.close()
