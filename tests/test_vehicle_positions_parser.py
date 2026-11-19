"""Parser + publisher wiring, with an in-memory FeedMessage (no network, no Kafka)."""

from google.transit import gtfs_realtime_pb2

from transit.gtfs_realtime.parsers import vehicle_positions
from transit.infra import topics


def _feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000

    e1 = feed.entity.add(id="e1")
    e1.vehicle.vehicle.id = "bus-1"
    e1.vehicle.trip.trip_id = "t1"
    e1.vehicle.trip.route_id = "51"
    e1.vehicle.position.latitude = 45.5
    e1.vehicle.position.longitude = -73.6
    e1.vehicle.current_status = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
    e1.vehicle.occupancy_status = gtfs_realtime_pb2.VehiclePosition.FULL
    e1.vehicle.timestamp = 1_700_000_000

    e2 = feed.entity.add(id="e2")  # minimal vehicle: optional fields unset
    e2.vehicle.vehicle.id = "bus-2"

    feed.entity.add(id="alert-only")  # no vehicle → skipped
    return feed


class FakePublisher:
    def __init__(self):
        self.calls = []

    def send_dataframe(self, topic, key_cols, df):
        self.calls.append((topic, key_cols, df))
        return len(df)


def test_parse_to_dataframe_columns_and_enums():
    df = vehicle_positions.VehiclePosition().parse_to_dataframe("STM", _feed())

    assert list(df.columns) == list(vehicle_positions.VEHICLE_DTYPES)
    assert len(df) == 2
    first = df.iloc[0]
    assert first["current_status"] == "STOPPED_AT"
    assert first["occupancy_status"] == "FULL"
    assert df.iloc[1][["trip_id", "latitude"]].isna().all()


def test_process_uses_shared_publisher():
    pub = FakePublisher()
    vehicle_positions.process("STM", _feed(), publisher=pub)

    (topic, key_cols, df), = pub.calls
    assert topic == topics.VEHICLE_POSITIONS
    assert key_cols == ["agency_id", "vehicle_id"]
    assert len(df) == 2
