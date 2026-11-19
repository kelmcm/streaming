"""Kafka client factories that read connection settings from `transit.settings`."""

from __future__ import annotations

from typing import Any

from confluent_kafka import Consumer, Producer

from transit.settings import get_kafka_settings


def make_producer(extra_config: dict[str, Any] | None = None) -> Producer:
    config = {
        "bootstrap.servers": get_kafka_settings().bootstrap_servers,
        "acks": "all",
        **(extra_config or {}),
    }
    return Producer(config)


def make_consumer(
    group_id: str,
    topics: list[str],
    extra_config: dict[str, Any] | None = None,
) -> Consumer:
    config = {
        "bootstrap.servers": get_kafka_settings().bootstrap_servers,
        "group.id": group_id,
        **(extra_config or {}),
    }
    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer
