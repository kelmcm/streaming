from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from google.transit import gtfs_realtime_pb2

from transit.infra import topics

if TYPE_CHECKING:
    from transit.gtfs_realtime.publisher import Publisher

logger = logging.getLogger(__name__)

VEHICLE_DTYPES = {
    "agency_id": "string",
    "entity_id": "string",
    "vehicle_id": "string",
    "trip_id": "string",
    "route_id": "string",
    "direction_id": "Int64",
    "latitude": "float64",
    "longitude": "float64",
    "bearing": "float64",
    "speed": "float64",
    "stop_id": "string",
    "current_stop_sequence": "Int64",
    "current_status": "Int64",
    "occupancy_status": "Int64",
    "congestion_level": "Int64",
    "timestamp": "Int64",
}

ENUM_CURRENT_STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}

ENUM_OCCUPANCY_STATUS = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS",
    7: "NO_DATA_AVAILABLE",
}


class VehiclePosition:
    """
    Class to hold a received VehiclePositions Feed from a GTFS Real-time API.
    """

    def _opt(self, container, field):
        """Return a field value only if the protobuf message actually set it."""
        return getattr(container, field) if container.HasField(field) else None

    def parse_to_dataframe(
        self, agency_id: str, feed: gtfs_realtime_pb2.FeedMessage, validate: bool = True
    ) -> pd.DataFrame:
        """Flatten VehiclePosition entities into a typed positions DataFrame."""
        records = []
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vp = entity.vehicle
            pos = vp.position
            records.append(
                {
                    "agency_id": agency_id,
                    "entity_id": entity.id,
                    "vehicle_id": vp.vehicle.id or None,
                    "trip_id": self._opt(vp.trip, "trip_id"),
                    "route_id": self._opt(vp.trip, "route_id"),
                    "direction_id": self._opt(vp.trip, "direction_id"),
                    "latitude": self._opt(pos, "latitude"),
                    "longitude": self._opt(pos, "longitude"),
                    "bearing": self._opt(pos, "bearing"),
                    "speed": self._opt(pos, "speed"),
                    "stop_id": self._opt(vp, "stop_id"),
                    "current_stop_sequence": self._opt(vp, "current_stop_sequence"),
                    "current_status": self._opt(vp, "current_status"),
                    "occupancy_status": self._opt(vp, "occupancy_status"),
                    "congestion_level": self._opt(vp, "congestion_level"),
                    "timestamp": self._opt(vp, "timestamp"),
                }
            )

        vehicle_positions = pd.DataFrame.from_records(records, columns=list(VEHICLE_DTYPES.keys())).astype(
            VEHICLE_DTYPES
        )

        # Clean enums
        vehicle_positions["current_status"] = vehicle_positions["current_status"].map(ENUM_CURRENT_STATUS)
        vehicle_positions["occupancy_status"] = vehicle_positions["occupancy_status"].map(
            ENUM_OCCUPANCY_STATUS
        )

        if validate:
            report = self.validate(vehicle_positions, feed.header.timestamp)
            logger.info("Validation report: %s", report)

        logger.info("Built positions frame with %d rows", len(vehicle_positions))
        return vehicle_positions

    def validate(self, vehicle_positions: pd.DataFrame, header_ts: int) -> dict:
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


def process(agency_id: str, feed: gtfs_realtime_pb2.FeedMessage, *, publisher: Publisher) -> pd.DataFrame:
    """Parse a VehiclePositions feed and publish each vehicle to Kafka using a shared publisher."""
    vp = VehiclePosition()
    vp_df = vp.parse_to_dataframe(agency_id, feed)
    publisher.send_dataframe(topic=topics.VEHICLE_POSITIONS, key_cols=["agency_id", "vehicle_id"], df=vp_df)
    return vp_df
