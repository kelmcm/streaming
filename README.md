# Streaming Transit Data

A streaming data pipeline for transit data.

This repository holds python packages and scripts that:
- **Poller** - Read and extract data from GTFS real-time transit feed. Sends data to a Kafka topic.
- **Producer** (Topic) - Receives messages from one or many GTFS feeds and publishes to topic. One topic per feed type.
- **Consumer** - 
  - Apache Flink - Joins the static data with real-time data & sends to Kafka topic
  - WebSocket Server - Subscribes to the Kafka topic and reads messages and pushes to web socket
- **Display** - Website that opens websocket and receives later


The static data model looks like...

![gtfs-erd.png](images/gtfs-erd.png)

This codebase used best practices from https://www.transit-data.com/.

## Repo Structure

The structure is in progress, working to migrate it to the following outline. 

```azure
streaming/
    ├── pyproject.toml              # name=transit, [project.scripts], dev deps, ruff/pytest config
    ├── uv.lock
    ├── .python-version
    ├── .env.example                # NEW – documented, committed; .env stays gitignored
    ├── compose.yaml                # renamed from docker-compose.yml (modern default name)
    ├── compose.override.yaml       # NEW – dev-only: hot reload + bind mounts (auto-loaded locally)
    ├── Dockerfile                  # multi-stage uv build, one image for all Python services
    ├── README.md
    ├── db/
    │   └── init/
    │       └── 001_schema.sql      # moved from src/sql/schema.sql; mounted into postgres initdb
    ├── frontend/                   # moved from src/display/ (not Python → not under src/)
    │   ├── index.html
    │   └── app.js
    ├── data/                       # gitignored GTFS downloads (unchanged)
    ├── docs/images/gtfs-erd.png    # moved from images/
    ├── src/
    │   └── transit/
    │       ├── __init__.py
    │       ├── settings.py         # os.environ + dataclasses: Kafka, Postgres, API keys (lazy, cached)
    │       ├── logging.py          # one logging setup used by every entry point
    │       ├── infra/              # adapters to external systems — no business logic
    │       │   ├── __init__.py
    │       │   ├── db.py           # psycopg_pool connection pool
    │       │   ├── kafka.py        # producer/consumer factories + JSON helpers
    │       │   └── topics.py       # topic-name constants (single source of truth)
    │       ├── realtime_gtfs/           # GTFS-RT → Kafka worker
    │       │   ├── __init__.py
    │       │   ├── feeds.py        # feed registry (was APIS dict in config.py)
    │       │   ├── poller.py
    │       │   ├── publisher.py    # was producer.py (MessageProducer)
    │       │   ├── parsers/
    │       │   │   ├── __init__.py
    │       │   │   ├── vehicle_positions.py
    │       │   │   ├── trip_updates.py
    │       │   │   └── service_alerts.py
    │       │   └── cli.py          # `transit-poller` entry point
    │       ├── static_gtfs/        # GTFS static → Postgres batch job
    │       │   ├── __init__.py
    │       │   ├── loader.py       # logic from scripts/ingest_gtfs.py
    │       │   └── cli.py          # `transit-ingest-static` entry point
    │       └── api/                # FastAPI web service
    │           ├── __init__.py
    │           ├── main.py         # create_app(), lifespan (pool open/close), mounts frontend
    │           ├── routes/
    │           │   ├── __init__.py
    │           │   ├── static.py   # was static_routes.py
    │           │   └── stream.py   # SSE endpoint (was consumer.py)
    │           └── services/
    │               ├── __init__.py
    │               └── vehicle_predictor.py
    └── tests/
        ├── conftest.py
        ├── test_settings.py
        ├── test_vehicle_positions_parser.py
        └── test_api_smoke.py
```

## Prerequiste

- Install Docker
- Get any API Keys and set in `src/config.py`
  
## Run 

In the root directory...

- Start Kafka, GTFS-Realtime producer for STM, and SSE consumer.
  ```bash
  docker compose up
  ```
  If you need to rebuild the docker containers: 
  ```bash
  docker compose up --build
  ```
- View the web app
  ```bash
  uv run python -m http.server 3000 --directory src/display 
  ```
  Access in browser at `http://localhost:3000`.
- Stop all Docker containers
  ```bash
  docker compose down
  ```
- Local postgres database:
  ```bash
  psql "$DATABASE_URL"
  ```

## Data Attribution & Terms of Use

Transit data is provided by the [Société de transport de Montréal (STM)](https://www.stm.info/en/about/developers) under the [Creative Commons Attribution 4.0 (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) licence. This is an unofficial project and is not affiliated with or endorsed by the STM.

- [ ] **TODO:** Add the STM attribution and unofficial-site disclaimer footer to the website (`src/display/index.html`) before publishing publicly.
