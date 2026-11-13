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

## Data Attribution & Terms of Use

Transit data is provided by the [Société de transport de Montréal (STM)](https://www.stm.info/en/about/developers) under the [Creative Commons Attribution 4.0 (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) licence. This is an unofficial project and is not affiliated with or endorsed by the STM.

- [ ] **TODO:** Add the STM attribution and unofficial-site disclaimer footer to the website (`src/display/index.html`) before publishing publicly.
