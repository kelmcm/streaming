from gtfs_realtime.utils.db import get_connection


class VehiclePredictor:

    def __init__(self):
        # Maps f"{agency_id}:{vehicle_id}" -> {trip_id, last_s, last_ts, ema_speed_mps, last_pred_s}
        self.state: dict[str, dict] = {}

    def get_shape_geom_frac(self,
        records: list[dict],
    ) -> dict[str, dict]:
        # Remove vehicle position data with missing information
        valid_trips = [
            r for r in records
            if r.get("trip_id") and r.get("longitude") is not None and r.get("latitude") is not None
        ]
        if not valid_trips:
            return {}

        # Construct lists of vehicle position data
        agency_ids = [str(r["agency_id"]) for r in valid_trips]
        vehicle_ids = [str(r["vehicle_id"]) for r in valid_trips]
        trip_ids = [str(r["trip_id"]) for r in valid_trips]
        lons = [float(r["longitude"]) for r in valid_trips]
        lats = [float(r["latitude"]) for r in valid_trips]

        # Combine vehicle positions with static trip and shape_geoms data
        query = """
        SELECT
            v.agency_id,
            v.vehicle_id,
            t.shape_id,
            sg.length_m,
            ST_LineLocatePoint(sg.geom, ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)) AS frac -- Fraction of route completed
        FROM UNNEST(%s::text[], %s::text[], %s::text[], %s::float8[], %s::float8[]) AS v(agency_id, vehicle_id, trip_id, lon, lat)
            INNER JOIN trips t ON t.agency_id = v.agency_id AND t.trip_id = v.trip_id
            INNER JOIN shape_geoms sg ON sg.agency_id = t.agency_id AND sg.shape_id = t.shape_id; \
        """

        results = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (agency_ids, vehicle_ids, trip_ids, lons, lats))
                for agency_id, vehicle_id, shape_id, length_m, frac in cur.fetchall():
                    results[f"{agency_id}:{vehicle_id}"] = {
                        "shape_id": shape_id,
                        "length_m": length_m,
                        "frac": frac,
                    }
        return results

    def enrich_batch(self,
         records: list[dict],
         horizon_sec: float = 25.0
    ) -> list[dict]:
        snapped = self.get_shape_geom_frac(records)
        slice_requests = []

        for record in records:
            key = f"{record.get('agency_id')}:{record.get('vehicle_id')}"
            if key not in snapped:
                continue

            shape_id = snapped[key]["shape_id"]
            length_m = snapped[key]["length_m"]
            frac = snapped[key]["frac"]

            # Compute current meters along route:
            s_t = frac * length_m
            ts = record.get("timestamp") or 0
            trip_id = str(record["trip_id"])
            prev = self.state.get(key)

            raw_speed = record.get("speed")
            gps_speed = min(max(float(raw_speed), 0.0), 22.0) if raw_speed is not None else None

            if prev and prev["trip_id"] == trip_id:
                delta_ts = ts - prev["last_ts"]
                if delta_ts > 0 and s_t >= prev["last_s"]:
                    obs_speed = min(max((s_t - prev["last_s"]) / delta_ts, 0.0), 22.0)
                    if gps_speed is not None:
                        ema_speed_mps = 0.4 * obs_speed + 0.3 * gps_speed + 0.3 * prev["ema_speed_mps"]
                    else:
                        ema_speed_mps = 0.6 * obs_speed + 0.4 * prev["ema_speed_mps"]
                    expected_s = min(prev["last_s"] + prev["ema_speed_mps"] * delta_ts, length_m)
                    pred_error_m = round(s_t - expected_s, 1)
                else:
                    ema_speed_mps = prev["ema_speed_mps"]
                    pred_error_m = prev["pred_error_m"]
            else:
                if gps_speed is not None:
                    ema_speed_mps = gps_speed
                else:
                    ema_speed_mps = 0.0 if record.get("current_status") == "STOPPED_AT" else 6.5
                pred_error_m = None

            if record.get("current_status") == "STOPPED_AT":
                effective_sec = horizon_sec * 0.5
            else:
                effective_sec = horizon_sec

            pred_s = min(s_t + ema_speed_mps * effective_sec, length_m)
            end_frac = min(pred_s / length_m, 1.0) if length_m > 0 else frac

            self.state[key] = {
                "trip_id": trip_id,
                "last_s": s_t,
                "last_ts": ts,
                "ema_speed_mps": ema_speed_mps,
                "last_pred_s": pred_s,
                "pred_error_m": pred_error_m,
            }
            record["est_speed_kmh"] = round(ema_speed_mps * 3.6, 1)
            record["pred_error_m"] = pred_error_m
            record["horizon_sec"] = horizon_sec

            if end_frac > frac + 1e-5:
                slice_requests.append({
                    "agency_id": str(record["agency_id"]),
                    "vehicle_id": str(record["vehicle_id"]),
                    "shape_id": shape_id,
                    "start_frac": frac,
                    "end_frac": end_frac,
                })

        paths = self.slice_paths(slice_requests)
        for record in records:
            key = f"{record.get('agency_id')}:{record.get('vehicle_id')}"
            record["predicted_path"] = paths.get(key)
        return records

    def slice_paths(self,
        slice_requests: list[dict],
    ) -> dict[str, list[list[float]]]:
        """

        :param slice_requests:
        :return: Dictionary of coordinates for the route
            {
                "STM:54012": [
                    [-73.56102, 45.51521],  # exact snapped position at start_frac
                    [-73.56045, 45.51580],  # intermediate street corner from shapes.txt
                    [-73.55951, 45.51662]   # exact predicted position at end_frac
                ]
            }
        """
        if not slice_requests:
            return {}

        agency_ids = [r["agency_id"] for r in slice_requests]
        vehicle_ids = [r["vehicle_id"] for r in slice_requests]
        shape_ids = [r["shape_id"] for r in slice_requests]
        start_fracs = [float(r["start_frac"]) for r in slice_requests]
        end_fracs = [float(r["end_frac"]) for r in slice_requests]

        # Get coordinates for the fraction of the route
        query = """
        SELECT
            v.agency_id,
            v.vehicle_id,
            ST_AsGeoJSON(ST_LineSubstring(sg.geom, v.start_frac, v.end_frac))::json->'coordinates' AS coords
        FROM UNNEST(%s::text[], %s::text[], %s::text[], %s::float8[], %s::float8[])
                 AS v(agency_id, vehicle_id, shape_id, start_frac, end_frac)
                 JOIN shape_geoms sg
                      ON sg.agency_id = v.agency_id AND sg.shape_id = v.shape_id; \
        """

        paths = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (agency_ids, vehicle_ids, shape_ids, start_fracs, end_fracs))
                for agency_id, vehicle_id, coords in cur.fetchall():
                    paths[f"{agency_id}:{vehicle_id}"] = coords
        return paths





