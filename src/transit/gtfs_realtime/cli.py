"""`transit-poller`: poll a GTFS-Realtime feed and publish decoded records to Kafka."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from functools import partial

from transit.gtfs_realtime.feeds import FEEDS
from transit.gtfs_realtime.parsers import vehicle_positions
from transit.gtfs_realtime.poller import Poller
from transit.gtfs_realtime.publisher import Publisher
from transit.log import configure_logging

logger = logging.getLogger(__name__)

# Feed kinds that have a parser + publisher implemented.
PROCESSORS = {
    "vehicle_positions": vehicle_positions.process,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="transit-poller", description=__doc__)
    parser.add_argument("--agency", choices=sorted(FEEDS), default="STM", help="Agency to poll")
    parser.add_argument(
        "--feed", choices=sorted(PROCESSORS), default="vehicle_positions", help="Feed kind to poll"
    )
    parser.add_argument("--interval", type=float, default=20.0, help="Seconds between polls")
    return parser.parse_args(argv)


def _exit_on_sigterm(signum, frame) -> None:
    # Docker stops containers with SIGTERM. Turning it into SystemExit lets the
    # Publisher context manager flush pending messages before the process exits.
    sys.exit(0)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_logging()
    signal.signal(signal.SIGTERM, _exit_on_sigterm)

    source = FEEDS[args.agency]
    url = source.url(args.feed)
    api_key = source.api_key()  # fails fast if the agency needs a key and it's missing

    logger.info("Polling %s %s every %.0fs", source.agency_id, args.feed, args.interval)
    try:
        with Publisher() as publisher:
            process = partial(PROCESSORS[args.feed], publisher=publisher)
            Poller(source.agency_id, url, process_func=process, api_key=api_key).poll(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Poller stopped")


if __name__ == "__main__":
    main()
