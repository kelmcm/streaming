"""Publishes DataFrames to Kafka as JSON records (was producer.py / MessageProducer)."""

from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from transit.infra.kafka import make_producer

logger = logging.getLogger(__name__)


def _delivery_callback(err, msg) -> None:
    """Per-message delivery report, triggered by poll()/flush() once delivery succeeds or fails."""
    if err:
        logger.error("Message failed delivery: %s", err)
    else:
        logger.debug("Produced event to %s: key=%s", msg.topic(), msg.key().decode("utf-8"))


class Publisher:
    """Long-lived Kafka producer. Create once per process; use as a context manager so
    pending messages are flushed on shutdown."""

    def __init__(self, extra_config: dict[str, Any] | None = None):
        self.producer = make_producer(extra_config)

    def send_dataframe(self, topic: str, key_cols: list[str], df: pd.DataFrame) -> int:
        """Send each row of dataframe as a JSON message to Kafka."""
        records = json.loads(df.to_json(orient="records"))
        for record in records:
            key = ":".join(str(record[col]) for col in key_cols)
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(record).encode("utf-8"),
                callback=_delivery_callback,
            )
            # Serve outstanding delivery report callbacks.
            self.producer.poll(0)

        # Block until this batch is delivered.
        self.producer.flush()
        logger.info("Published %d records to %s", len(records), topic)
        return len(records)

    def close(self, timeout: float = 10.0) -> None:
        remaining = self.producer.flush(timeout)
        if remaining:
            logger.warning("%d messages still undelivered at shutdown", remaining)

    def __enter__(self) -> Publisher:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
