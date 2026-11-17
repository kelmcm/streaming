from functools import lru_cache
from fastapi import APIRouter
from gtfs_realtime.utils.db import get_connection

router = APIRouter(prefix="/static", tags=["static"])


def _format_hex_color(color: str | None, default: str) -> str:
    if not color:
        return default
    color = color.strip()
    return color if color.startswith("#") else f"#{color}"


@lru_cache(maxsize=8)
def _load_routes_geojson(agency_id: str | None) -> dict:
    query = """
        WITH shape_routes AS (
            SELECT DISTINCT agency_id, shape_id, route_id
            FROM trips
            WHERE shape_id IS NOT NULL
              AND (%s::text IS NULL OR agency_id = %s)
        )
        SELECT
            sr.agency_id,
            sr.route_id,
            sr.shape_id,
            r.route_short_name,
            r.route_long_name,
            r.route_type,
            r.route_color,
            r.route_text_color,
            ST_AsGeoJSON(ST_SimplifyPreserveTopology(sg.geom, 0.00005))::json AS geometry
        FROM shape_routes sr
        JOIN shape_geoms sg
          ON sg.agency_id = sr.agency_id AND sg.shape_id = sr.shape_id
        JOIN routes r
          ON r.agency_id = sr.agency_id AND r.route_id = sr.route_id
        ORDER BY r.route_type DESC, sr.route_id;
    """

    features = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (agency_id, agency_id))
            for (
                ag_id,
                route_id,
                shape_id,
                short_name,
                long_name,
                route_type,
                route_color,
                route_text_color,
                geometry,
            ) in cur.fetchall():
                features.append(
                    {
                        "type": "Feature",
                        "properties": {
                            "agency_id": ag_id,
                            "route_id": route_id,
                            "shape_id": shape_id,
                            "route_short_name": short_name,
                            "route_long_name": long_name,
                            "route_type": route_type,
                            "route_color": _format_hex_color(route_color, "#78b7da"),
                            "route_text_color": _format_hex_color(route_text_color, "#ffffff"),
                        },
                        "geometry": geometry,
                    }
                )

    return {"type": "FeatureCollection", "features": features}


@lru_cache(maxsize=8)
def _load_stops_geojson(agency_id: str | None) -> dict:
    query = """
        SELECT
            agency_id,
            stop_id,
            stop_code,
            stop_name,
            stop_lon,
            stop_lat,
            location_type
        FROM stops
        WHERE stop_lon IS NOT NULL
          AND stop_lat IS NOT NULL
          AND (%s::text IS NULL OR agency_id = %s);
    """

    features = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (agency_id, agency_id))
            for ag_id, stop_id, stop_code, stop_name, lon, lat, location_type in cur.fetchall():
                features.append(
                    {
                        "type": "Feature",
                        "properties": {
                            "agency_id": ag_id,
                            "stop_id": stop_id,
                            "stop_code": stop_code,
                            "stop_name": stop_name,
                            "location_type": location_type,
                        },
                        "geometry": {
                            "type": "Point",
                            "coordinates": [lon, lat],
                        },
                    }
                )

    return {"type": "FeatureCollection", "features": features}


@router.get("/routes")
def get_static_routes(agency_id: str | None = None):
    """Return static GTFS route geometries as a GeoJSON FeatureCollection."""
    return _load_routes_geojson(agency_id)


@router.get("/stops")
def get_static_stops(agency_id: str | None = None):
    """Return static GTFS stops as a GeoJSON FeatureCollection."""
    return _load_stops_geojson(agency_id)
