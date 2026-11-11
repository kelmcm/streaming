from gtfs_realtime.config import API_VEHICLE_POSITIONS, API_KEY
from gtfs_realtime.parsers.vehicle_positions import VehiclePosition
from gtfs_realtime.producer import producer, send_message

import time
import logging
from dataclasses import dataclass
from typing import Callable, Optional

import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

@dataclass
class PollState:
    """
    Validators and dedup cursor carried across poll iterations.
    """
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    last_header_ts: int = 0


def poll_once(
    session: requests.Session,
    url: str,
    state: PollState,
    process_snapshot: Callable[[gtfs_realtime_pb2.FeedMessage], None],
    default_interval: float = 20.0,
) -> float:
    """
    Perform one conditional poll. Returns the number of seconds to wait
    before the next poll (from Cache-Control max-age when available).
    """
    headers = {}
    if state.etag:
        headers["If-None-Match"] = state.etag
    if state.last_modified:
        headers["If-Modified-Since"] = state.last_modified

    headers.update({
        'accept': 'application/x-protobuf',
        'apiKey': API_KEY # Environment variables
    })

    response = session.get(url, headers=headers, timeout=15.0)

    # Derive the next interval from the producer's own cache directive
    interval = default_interval
    cache_control = response.headers.get("Cache-Control", "")
    if "max-age=" in cache_control:
        try:
            interval = max(5.0, float(cache_control.split("max-age=")[1].split(",")[0]))
        except (IndexError, ValueError):
            interval = default_interval

    if response.status_code == 304:
        logging.info("304 Not Modified — skipping download and decode")
        return interval

    response.raise_for_status()

    # Persist validators for the next conditional request
    state.etag = response.headers.get("ETag", state.etag)
    state.last_modified = response.headers.get("Last-Modified", state.last_modified)

    # Decode and apply the content-level dedup guard
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except DecodeError:
        logging.error("Body was not valid protobuf — treating as transient, will retry")
        return interval

    header_ts = feed.header.timestamp
    if header_ts <= state.last_header_ts:
        logging.info("Header timestamp %d not newer than last %d — dropping duplicate",
                     header_ts, state.last_header_ts)
        return interval

    state.last_header_ts = header_ts
    logging.info("New snapshot: header_ts=%d, entities=%d", header_ts, len(feed.entity))
    process_snapshot(feed)
    return interval


def run_poller(
    url: str,
    process_snapshot: Callable[[gtfs_realtime_pb2.FeedMessage], None],
    max_backoff: float = 300.0,
) -> None:
    """
    Poll forever with conditional requests and exponential error backoff.
    """
    session = requests.Session()
    session.headers["User-Agent"] = "transit-data-poller/1.0"
    state = PollState()
    backoff = 0.0

    while True:
        try:
            interval = poll_once(session, url, state, process_snapshot)
            backoff = 0.0  # reset on any successful round-trip
            time.sleep(interval)
        except requests.RequestException as exc:
            backoff = min(max(backoff * 2, 5.0), max_backoff)
            logging.warning("Transport error (%s) — backing off %.0f s", exc, backoff)
            time.sleep(backoff)

def process_vehicle_positions(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    vp = VehiclePosition()
    vp_df = vp.parse_to_dataframe(feed)
    send_message(producer, "vehicle-positions", vp_df)
    return vp_df


if __name__ == "__main__":
    run_poller(API_VEHICLE_POSITIONS, process_vehicle_positions)
