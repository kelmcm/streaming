import pandas as pd

def validate_vehicle_frame(vehicle_positions: pd.DataFrame, header_ts: int) -> dict:
    """Validate a decoded vehicle-position frame against structural expectations."""
    assert not vehicle_positions.empty, "Decoded frame is empty — feed may be down"

    # entity_id must be unique within a single FeedMessage
    dupes = vehicle_positions["entity_id"].duplicated().sum()
    assert dupes == 0, f"{dupes} duplicate entity_id values in one message"

    # Coordinates, where present, must fall within WGS84 bounds
    coords = vehicle_positions.dropna(subset=["latitude", "longitude"])
    assert coords["latitude"].between(-90, 90).all(), "latitude out of WGS84 bounds"
    assert coords["longitude"].between(-180, 180).all(), "longitude out of WGS84 bounds"

    # Per-vehicle timestamps should not lead the header timestamp
    ahead = (vehicle_positions["timestamp"].dropna() > header_ts + 5).sum()

    results = {
        "rows": len(vehicle_positions),
        "with_position": len(coords),
        "with_trip_id": int(vehicle_positions["trip_id"].notna().sum()),
        "timestamps_ahead_of_header": int(ahead),
    }
    return results

def match_trip_updates_to_static(
        trip_updates: pd.DataFrame,      # cols: trip_id, stop_id, delay
        stop_times: pd.DataFrame,        # static stop_times.txt
) -> pd.DataFrame:
    stop_times = stop_times.astype({"trip_id": "string", "stop_id": "string"})
    trip_updates = trip_updates.astype({"trip_id": "string", "stop_id": "string"})
    merged = stop_times.merge(
        trip_updates,
        on=["trip_id", "stop_id"],
        how="left",
        validate="one_to_one",
    )
    unmatched = trip_updates.merge(
        stop_times[["trip_id"]].drop_duplicates(), on="trip_id", how="left", indicator=True
    )
    orphan_rate = (unmatched["_merge"] == "left_only").mean()
    if orphan_rate > 0.05:
        logging.warning("high unmatched trip_id rate: %.1f%%", orphan_rate * 100)
    return merged