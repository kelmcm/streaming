"""Registry of GTFS-Realtime feed sources, keyed by agency (was the APIS dict in config.py)."""

from __future__ import annotations

from dataclasses import dataclass

from transit.settings import get_api_key


@dataclass(frozen=True)
class FeedSource:
    agency_id: str
    urls: dict[str, str]  # feed kind (e.g. "vehicle_positions") -> URL
    api_key_env: str | None = None  # env var holding the API key, if the feed needs one

    def url(self, feed: str) -> str:
        try:
            return self.urls[feed]
        except KeyError:
            raise ValueError(
                f"Agency {self.agency_id} has no '{feed}' feed (available: {', '.join(self.urls)})"
            ) from None

    def api_key(self) -> str | None:
        """Resolve the API key from the environment; raises if it is required but missing."""
        return get_api_key(self.api_key_env) if self.api_key_env else None


FEEDS: dict[str, FeedSource] = {
    "STM": FeedSource(
        agency_id="STM",
        urls={
            "vehicle_positions": "https://api.stm.info/pub/od/gtfs-rt/ic/v2/vehiclePositions",
            "trip_updates": "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates",
            "service_alerts": "https://api.stm.info/pub/od/i3/v2/messages/etatservice",
        },
        api_key_env="STM_API_KEY",
    ),
    "MBTA": FeedSource(
        agency_id="MBTA",
        urls={"vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.pb"},
    ),
}
