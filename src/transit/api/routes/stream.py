"""SSE endpoint streaming enriched vehicle positions from Kafka."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

from fastapi import APIRouter
from fastapi.sse import EventSourceResponse, ServerSentEvent

from transit.api.services.vehicle_predictor import VehiclePredictor
from transit.infra import topics
from transit.infra.kafka import make_consumer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["stream"])
predictor = VehiclePredictor()


@router.get("/vehicle_positions", response_class=EventSourceResponse)
async def stream_vehicle_positions():
    consumer = make_consumer(
        group_id=f"gtfs-vehicle-positions-{uuid.uuid4()}",
        topics=[topics.VEHICLE_POSITIONS],
        extra_config={
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        },
    )

    try:
        while True:
            # Grab up to 500 messages at once without blocking the event loop
            msgs = await asyncio.to_thread(consumer.consume, 500, 0.2)

            if not msgs:
                continue

            records = []
            for msg in msgs:
                if msg.error():
                    logger.error("Kafka consumer error: %s", msg.error())
                    continue
                records.append(json.loads(msg.value().decode("utf-8")))

            if records:
                enriched = await asyncio.to_thread(predictor.enrich_batch, records)
                for record in enriched:
                    yield ServerSentEvent(raw_data=json.dumps(record))

    finally:
        consumer.close()
