"""One logging setup shared by every entry point (CLIs and the API)."""

from __future__ import annotations

import logging
import os

_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(level: str | None = None) -> None:
    """Configure root logging once. Level defaults to $LOG_LEVEL or INFO."""
    logging.basicConfig(
        level=(level or os.environ.get("LOG_LEVEL", "INFO")).upper(),
        format=_FORMAT,
    )
