"""Smoke tests for the FastAPI application, route registration, and static frontend mount."""

from pathlib import Path

from fastapi.testclient import TestClient

from transit.api.main import create_app
from transit.infra import db


def test_api_routes_and_frontend_served(monkeypatch):
    # Stub pool open/close so smoke test doesn't require a live Postgres instance
    monkeypatch.setattr(db, "open_pool", lambda: None)
    monkeypatch.setattr(db, "close_pool", lambda: None)

    app = create_app(frontend_dir=Path("frontend"))
    paths = set(app.openapi()["paths"])
    assert "/health" in paths
    assert "/api/static/routes" in paths
    assert "/api/static/stops" in paths
    assert "/api/stream/vehicle_positions" in paths

    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}

        index = client.get("/")
        assert index.status_code == 200
        assert "maplibre" in index.text.lower()
