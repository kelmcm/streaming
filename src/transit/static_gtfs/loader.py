"""
Repeatable multi-agency GTFS static ingestion pipeline using Partridge, Pandas, and Postgres COPY.

Run through the CLI:
    uv run transit-ingest-static --path data/GTFS_STM --agency-id STM
"""

from __future__ import annotations

import io
import logging
import time
import zipfile
from pathlib import Path

import pandas as pd
import partridge as ptg
import psycopg

logger = logging.getLogger(__name__)

# Relative to the working directory: the repo root locally, /app in the Docker image.
DEFAULT_SCHEMA_PATH = Path("db/init/001_schema.sql")

# Target columns for each table (excluding agency_id, which is prepended automatically)
TABLE_COLUMNS: dict[str, list[str]] = {
    "agency": [
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
        "agency_fare_url",
    ],
    "feed_info": [
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_start_date",
        "feed_end_date",
        "feed_version",
    ],
    "calendar": [
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ],
    "calendar_dates": [
        "service_id",
        "date",
        "exception_type",
    ],
    "routes": [
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_url",
        "route_color",
        "route_text_color",
        "route_desc",
        "route_desc_detail",
    ],
    "route_patterns": [
        "route_pattern_id",
        "route_id",
        "direction_id",
        "route_pattern_typicality",
    ],
    "directions": [
        "route_direction_id",
        "route_id",
        "direction_id",
        "direction",
        "direction_legacy",
    ],
    "shapes": [
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
        "route_pattern_id",
    ],
    "stops": [
        "stop_id",
        "stop_code",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "stop_url",
        "location_type",
        "parent_station",
        "wheelchair_boarding",
    ],
    "trips": [
        "trip_id",
        "route_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "shape_id",
        "wheelchair_accessible",
        "route_pattern_id",
    ],
    "stop_times": [
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "pickup_type",
    ],
    "translations": [
        "table_name",
        "field_name",
        "language",
        "record_id",
        "translation",
    ],
}

INTEGER_COLUMNS: set[str] = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "exception_type",
    "route_type",
    "direction_id",
    "route_pattern_typicality",
    "shape_pt_sequence",
    "location_type",
    "wheelchair_boarding",
    "wheelchair_accessible",
    "stop_sequence",
    "pickup_type",
}


def build_partridge_config():
    """
    Build an extended Partridge dependency graph that:
    1. Adds custom GTFS tables (`route_patterns.txt`, `directions.txt`, `translations.txt`)
       with foreign-key pruning against `routes.txt`.
    2. Keeps `stops.txt` unpruned by `stop_times.txt` so parent stations (location_type=1)
       are preserved.
    """
    config = ptg.config.default_config()

    # Keep all stops (including parent stations not directly visited in stop_times.txt)
    if config.has_edge("stops.txt", "stop_times.txt"):
        config.remove_edge("stops.txt", "stop_times.txt")

    # Add custom GTFS extension files
    config.add_node(
        "route_patterns.txt",
        required_columns=("route_pattern_id", "route_id"),
    )
    config.add_edge(
        "route_patterns.txt",
        "routes.txt",
        dependencies=[{"route_patterns.txt": "route_id", "routes.txt": "route_id"}],
    )

    config.add_node(
        "directions.txt",
        required_columns=("route_direction_id", "route_id"),
    )
    config.add_edge(
        "directions.txt",
        "routes.txt",
        dependencies=[{"directions.txt": "route_id", "routes.txt": "route_id"}],
    )

    config.add_node(
        "translations.txt",
        required_columns=("table_name", "field_name", "language", "record_id"),
    )

    return config


def normalize_dataframe(
    df: pd.DataFrame,
    agency_id: str,
    columns: list[str],
) -> pd.DataFrame:
    """Ensure all expected columns exist, cast integer columns cleanly, and prepend agency_id."""
    out = pd.DataFrame(index=df.index)
    out["agency_id"] = agency_id

    for col in columns:
        if col in df.columns:
            series = df[col]
            if col in INTEGER_COLUMNS:
                # Use pandas nullable integer so NaNs serialize as empty strings in CSV
                series = pd.to_numeric(series, errors="coerce").astype("Int64")
            out[col] = series
        else:
            out[col] = pd.NA

    return out


def copy_dataframe(
    cur: psycopg.Cursor,
    table_name: str,
    df: pd.DataFrame,
) -> int:
    """Bulk-load a normalized DataFrame into Postgres using COPY FROM STDIN."""
    if df.empty:
        return 0

    cols_sql = ", ".join(df.columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    with cur.copy(f"COPY {table_name} ({cols_sql}) FROM STDIN WITH (FORMAT csv)") as copy:
        while data := buf.read(1024 * 1024):
            copy.write(data)

    return len(df)


def copy_stop_times_streaming(
    cur: psycopg.Cursor,
    gtfs_path: Path,
    agency_id: str,
    valid_trip_ids: set[str],
    valid_stop_ids: set[str],
    chunksize: int = 500_000,
) -> int:
    """
    Stream `stop_times.txt` in chunks, pruning any orphaned trip_id/stop_id rows
    against Partridge's validated sets, and bulk-copying into Postgres.
    """
    columns = TABLE_COLUMNS["stop_times"]
    cols_sql = ", ".join(["agency_id", *columns])
    total_rows = 0

    def open_stop_times():
        if gtfs_path.is_dir():
            return open(gtfs_path / "stop_times.txt", encoding="utf-8-sig")
        zf = zipfile.ZipFile(gtfs_path)
        return io.TextIOWrapper(zf.open("stop_times.txt"), encoding="utf-8-sig")

    with open_stop_times() as f:
        reader = pd.read_csv(f, dtype=str, chunksize=chunksize)
        with cur.copy(
            f"COPY stop_times ({cols_sql}) FROM STDIN WITH (FORMAT csv)"
        ) as copy:
            for chunk in reader:
                chunk.columns = chunk.columns.str.strip()
                # Relational integrity pruning against Partridge's validated trips & stops
                mask = chunk["trip_id"].isin(valid_trip_ids) & chunk["stop_id"].isin(
                    valid_stop_ids
                )
                filtered = chunk.loc[mask]
                if filtered.empty:
                    continue

                norm = normalize_dataframe(filtered, agency_id, columns)
                buf = io.StringIO()
                norm.to_csv(buf, index=False, header=False, na_rep="")
                copy.write(buf.getvalue())
                total_rows += len(norm)

    return total_rows


def ingest_gtfs(
    gtfs_path: Path,
    agency_id: str | None,
    conninfo: str,
    schema_path: Path = DEFAULT_SCHEMA_PATH,
) -> None:
    t0 = time.perf_counter()
    schema_sql = schema_path.read_text(encoding="utf-8")  # fail before the slow feed load
    config = build_partridge_config()
    feed = ptg.load_feed(str(gtfs_path), config=config)

    agency_df = feed.agency.copy()
    if agency_id is None:
        if "agency_id" in agency_df.columns and not agency_df["agency_id"].isna().all():
            agency_id = str(agency_df["agency_id"].iloc[0]).strip()
        else:
            raise ValueError(
                "Feed agency.txt does not specify an agency_id; pass --agency-id explicitly."
            )

    logger.info("Ingesting GTFS feed from '%s' for agency_id='%s'...", gtfs_path, agency_id)

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            # 1. Ensure schema exists
            cur.execute(schema_sql)

            # 2. Remove existing rows for this agency (cascades cleanly across all child tables)
            cur.execute("DELETE FROM agency WHERE agency_id = %s", (agency_id,))

            # 3. Check if stop_times is completely empty so we can build constraints after COPY
            cur.execute("SELECT EXISTS (SELECT 1 FROM stop_times LIMIT 1)")
            stop_times_has_other_agencies = cur.fetchone()[0]

            if not stop_times_has_other_agencies:
                cur.execute(
                    """
                    ALTER TABLE stop_times DROP CONSTRAINT IF EXISTS stop_times_pkey;
                    ALTER TABLE stop_times DROP CONSTRAINT IF EXISTS stop_times_agency_id_fkey;
                    ALTER TABLE stop_times DROP CONSTRAINT IF EXISTS stop_times_agency_id_trip_id_fkey;
                    ALTER TABLE stop_times DROP CONSTRAINT IF EXISTS stop_times_agency_id_stop_id_fkey;
                    """
                )

            # 4. Load all tables except stop_times via Partridge
            for table_name, cols in TABLE_COLUMNS.items():
                if table_name == "stop_times":
                    continue

                t_table = time.perf_counter()
                df = feed.get(f"{table_name}.txt")
                if df is None or df.empty:
                    logger.info(f"  - {table_name:<16} skipped (not present or empty)")
                    continue

                norm_df = normalize_dataframe(df, agency_id, cols)
                count = copy_dataframe(cur, table_name, norm_df)
                elapsed = time.perf_counter() - t_table
                logger.info(f"  - {table_name:<16} loaded {count:>10,} rows ({elapsed:.2f}s)")

            # 5. Stream stop_times.txt validated against Partridge's pruned trips & stops
            t_st = time.perf_counter()
            valid_trip_ids = set(feed.trips["trip_id"].astype(str))
            valid_stop_ids = set(feed.stops["stop_id"].astype(str))
            st_count = copy_stop_times_streaming(
                cur,
                gtfs_path,
                agency_id,
                valid_trip_ids,
                valid_stop_ids,
            )
            st_elapsed = time.perf_counter() - t_st
            logger.info(f"  - {'stop_times':<16} loaded {st_count:>10,} rows ({st_elapsed:.2f}s)")

            if not stop_times_has_other_agencies:
                t_idx = time.perf_counter()
                logger.info("  - Building primary & foreign key constraints on stop_times...")
                cur.execute(
                    """
                    ALTER TABLE stop_times
                        ADD PRIMARY KEY (agency_id, trip_id, stop_sequence),
                        ADD FOREIGN KEY (agency_id) REFERENCES agency(agency_id) ON DELETE CASCADE,
                        ADD FOREIGN KEY (agency_id, trip_id) REFERENCES trips(agency_id, trip_id)
                            ON DELETE CASCADE,
                        ADD FOREIGN KEY (agency_id, stop_id) REFERENCES stops(agency_id, stop_id)
                            ON DELETE CASCADE;
                    """
                )
                logger.info(f"  - Constraints verified in {time.perf_counter() - t_idx:.2f}s")

            # 7. Insert connected shape points into shape_geoms
            cur.execute(
                """
                INSERT INTO shape_geoms (agency_id, shape_id, geom, length_m)
                SELECT
                    agency_id,
                    shape_id,
                    ST_MakeLine(
                            ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326)
                                ORDER BY shape_pt_sequence
                    ) AS geom,
                    ST_Length(
                            ST_MakeLine(
                                    ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326)
                                        ORDER BY shape_pt_sequence
                            )::geography
                    ) AS length_m
                FROM shapes
                WHERE agency_id = %s
                GROUP BY agency_id, shape_id;
                """,
                (agency_id,),
            )

        conn.commit()

    logger.info("Done in %.2fs!", time.perf_counter() - t0)
