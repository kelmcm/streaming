"""Postgres access through a shared psycopg connection pool.

The API opens/closes the pool in its lifespan. CLIs can call `open_pool()` / `close_pool()`
explicitly; `get_connection()` also opens the pool lazily on first use.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool

from transit.settings import get_postgres_settings

logger = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


def open_pool(conninfo: str | None = None, *, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    """Create the module-level pool if it doesn't exist yet, and return it."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo or get_postgres_settings().conninfo,
            min_size=min_size,
            max_size=max_size,
            open=True,
            name="transit",
        )
        logger.info("Opened Postgres connection pool (min=%d, max=%d)", min_size, max_size)
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("Closed Postgres connection pool")


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    """Borrow a connection from the pool (same call shape as the old utils.db helper)."""
    pool = _pool or open_pool()
    with pool.connection() as conn:
        yield conn
