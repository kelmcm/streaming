"""Runtime configuration, read from environment variables (and `.env` for local runs).

Settings are split by concern and loaded lazily, so nothing fails at import time and each
process only validates the variables it actually uses:

- poller:  Kafka settings + the feed's API key (e.g. STM_API_KEY)
- api:     Kafka + Postgres settings
- ingest:  Postgres settings only

`load_dotenv()` never overrides variables that are already set, so values injected by
docker compose (`environment:` / `env_file:`) take priority over a local `.env`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name} (see .env.example)")
    return value


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)  # never printed in logs/tracebacks

    @property
    def conninfo(self) -> str:
        """libpq connection string; safely escapes special characters in the password."""
        return make_conninfo(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password
        )


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str


@lru_cache
def get_postgres_settings() -> PostgresSettings:
    load_dotenv()
    return PostgresSettings(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=_require("POSTGRES_DB"),
        user=_require("POSTGRES_USER"),
        password=_require("POSTGRES_PASSWORD"),
    )


@lru_cache
def get_kafka_settings() -> KafkaSettings:
    load_dotenv()
    return KafkaSettings(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))


def get_api_key(env_var: str) -> str:
    """Look up a feed API key. Only the poller calls this, for the agency it is polling."""
    load_dotenv()
    return _require(env_var)
