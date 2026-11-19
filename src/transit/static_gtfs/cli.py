"""`transit-ingest-static`: load a GTFS static feed (directory or .zip) into Postgres."""

from __future__ import annotations

import argparse
from pathlib import Path

from transit.log import configure_logging
from transit.settings import get_postgres_settings
from transit.static_gtfs.loader import DEFAULT_SCHEMA_PATH, ingest_gtfs


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="transit-ingest-static", description=__doc__)
    parser.add_argument(
        "--path",
        required=True,
        type=Path,
        help="Path to the GTFS directory or .zip archive (e.g., data/GTFS_STM)",
    )
    parser.add_argument(
        "--agency-id",
        default=None,
        help="Override agency_id (defaults to agency_id from agency.txt)",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="Postgres connection URL/conninfo (defaults to POSTGRES_* settings)",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help=f"Schema SQL applied before loading (default: {DEFAULT_SCHEMA_PATH})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_logging()
    conninfo = args.db_url or get_postgres_settings().conninfo
    ingest_gtfs(args.path, args.agency_id, conninfo, schema_path=args.schema)


if __name__ == "__main__":
    main()
