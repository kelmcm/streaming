"""FastAPI application factory, lifespan management, and frontend static file mounting."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from transit.api.routes import static, stream
from transit.infra import db
from transit.log import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    db.open_pool()
    try:
        yield
    finally:
        db.close_pool()


def create_app(frontend_dir: Path | None = None) -> FastAPI:
    app = FastAPI(title="Transit Stream", lifespan=lifespan)

    @app.get("/health")
    def health_check() -> dict[str, str]:
        """Liveness check (used by container healthchecks / any future deploy target)."""
        return {"status": "ok"}

    app.include_router(static.router)  # /api/static/routes, /api/static/stops
    app.include_router(stream.router)  # /api/stream/vehicle_positions

    resolved_frontend = (
        frontend_dir if frontend_dir is not None else Path(os.getenv("FRONTEND_DIR", "frontend"))
    )
    if resolved_frontend.is_dir():
        app.mount("/", StaticFiles(directory=resolved_frontend, html=True), name="frontend")

    return app


app = create_app()
