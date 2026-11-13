import time
import logging
from typing import Callable

import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

class Poller:

    def __init__(self,
            agency_id: str,
            url: str,
            process_func: Callable[[gtfs_realtime_pb2.FeedMessage], None],
            api_key: str = None,
        ):
        self.agency_id = agency_id
        self.url = url
        self.process_func = process_func
        self.api_key = api_key

    def _poll_once(
        self,
        session: requests.Session,
        last_header_ts: int,
    ) -> int:
        """
        Perform one poll. Returns the latest header timestamp seen so far.
        """
        headers = {
            "accept": "application/x-protobuf",
        }
        if self.api_key:
            headers["apiKey"] = self.api_key

        response = session.get(self.url, headers=headers, timeout=15.0)
        response.raise_for_status()

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
        self.process_func(self.agency_id, feed)
        return header_ts


    def poll(
        self,
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
                last_header_ts = self._poll_once(session, last_header_ts)
                backoff = 0.0  # reset on any successful round-trip
                time.sleep(poll_interval)
            except requests.RequestException as exc:
                backoff = min(max(backoff * 2, 5.0), max_backoff)
                logging.warning("Transport error (%s) — backing off %.0f s", exc, backoff)
                time.sleep(backoff)
