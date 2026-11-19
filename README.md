# Streaming Transit Data

A streaming data pipeline for transit data (Kafka, PostGIS, FastAPI/SSE, MapLibre).

This repository holds an installable Python package (`transit`) with three runtime components:
- **Realtime Poller (`transit-poller`)** — Polls GTFS-Realtime feeds (e.g. STM vehicle positions) and publishes records to Kafka.
- **Static Loader (`transit-ingest-static`)** — Bulk-loads static GTFS feeds into PostGIS and builds connected route geometry shapes.
- **Web API & UI (`transit.api.main:app`)** — Serves static route/stop GeoJSON from PostGIS, enriches real-time vehicle positions from Kafka with predicted street-snapped trajectories over Server-Sent Events (SSE), and hosts the MapLibre web UI.

The static data model looks like:

![gtfs-erd.png](docs/images/gtfs-erd.png)

This codebase uses best practices from https://www.transit-data.com/.

## Prerequisites

- Docker & Docker Compose (or Python 3.13 + `uv` for local runs)
- Copy `.env.example` to `.env` and fill in `STM_API_KEY`:
  ```bash
  cp .env.example .env
  ```

## Run with Docker Compose

- Start Kafka, Kafka-UI, PostGIS, the GTFS-Realtime poller, and the Web API (with hot reload):
  ```bash
  docker compose up --build
  ```
- Load static GTFS data into PostGIS (one-time or when static feed updates):
  ```bash
  docker compose run --rm ingest --path data/GTFS_STM --agency-id STM
  ```
- Open in browser:
  - **Map UI & API:** `http://localhost:8000`
  - **Kafka UI:** `http://localhost:8080`
- Stop all containers:
  ```bash
  docker compose down
  ```

## Run Locally with `uv`

```bash
uv sync
uv run transit-ingest-static --path data/GTFS_STM --agency-id STM
uv run transit-poller --agency STM
uv run uvicorn transit.api.main:app --reload --port 8000
```

## Tests & Linting

```bash
uv run pytest
uv run ruff check
```

## Data Attribution & Terms of Use

Transit data is provided by the [Société de transport de Montréal (STM)](https://www.stm.info/en/about/developers) under the [Creative Commons Attribution 4.0 (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) licence. This is an unofficial project and is not affiliated with or endorsed by the STM.

- [ ] **TODO:** Add the STM attribution and unofficial-site disclaimer footer to the website (`frontend/index.html`) before publishing publicly.
