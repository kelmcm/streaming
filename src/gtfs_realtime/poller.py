from gtfs_realtime.config import API_VEHICLE_POSITIONS, API_KEY
from gtfs_realtime.parsers.vehicle_positions import process as process_vehicle_positions

import time
import logging
from typing import Callable

import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def poll_once(
    session: requests.Session,
    url: str,
    last_header_ts: int,
    process_func: Callable[[gtfs_realtime_pb2.FeedMessage], None],
) -> int:
    """
    Perform one poll. Returns the latest header timestamp seen so far.
    """
    headers = {
        "accept": "application/x-protobuf",
        "apiKey": API_KEY,  # Environment variables
    }

    response = session.get(url, headers=headers, timeout=15.0)
    response.raise_for_status()

    # TODO: Revisit response headers (ETag / If-Modified-Since)

    # Decode and apply the content-level dedup guard
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except DecodeError:
        logging.error("Body was not valid protobuf — treating as transient, will retry")
        return last_header_ts

    header_ts = feed.header.timestamp
    if header_ts <= last_header_ts:
        logging.info(
            "Header timestamp %d not newer than last %d — dropping duplicate",
            header_ts,
            last_header_ts,
        )
        return last_header_ts

    logging.info("New snapshot: header_ts=%d, entities=%d", header_ts, len(feed.entity))
    process_func(feed)
    return header_ts


def run_poller(
    url: str,
    process_func: Callable[[gtfs_realtime_pb2.FeedMessage], None],
    poll_interval: float = 20.0,
    max_backoff: float = 300.0,
) -> None:
    """
    Poll forever with timestamp deduplication and exponential error backoff.
    """
    session = requests.Session()
    session.headers["User-Agent"] = "transit-data-poller/1.0"
    last_header_ts = 0
    backoff = 0.0

    while True:
        try:
            last_header_ts = poll_once(session, url, last_header_ts, process_func)
            backoff = 0.0  # reset on any successful round-trip
            time.sleep(poll_interval)
        except requests.RequestException as exc:
            backoff = min(max(backoff * 2, 5.0), max_backoff)
            logging.warning("Transport error (%s) — backing off %.0f s", exc, backoff)
            time.sleep(backoff)


if __name__ == "__main__":
    run_poller(API_VEHICLE_POSITIONS, process_vehicle_positions)
