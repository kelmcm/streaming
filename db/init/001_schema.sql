-- Multi-agency GTFS Static Schema
-- Every table includes `agency_id` as a foreign key to `agency(agency_id) ON DELETE CASCADE`
-- and uses composite primary/foreign keys scoped by `agency_id`.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS agency (
    agency_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    agency_phone TEXT,
    agency_fare_url TEXT
);

CREATE TABLE IF NOT EXISTS feed_info (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    feed_publisher_name TEXT NOT NULL,
    feed_publisher_url TEXT,
    feed_lang TEXT,
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version TEXT,
    PRIMARY KEY (agency_id, feed_publisher_name)
);

CREATE TABLE IF NOT EXISTS calendar (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    service_id TEXT NOT NULL,
    monday SMALLINT NOT NULL,
    tuesday SMALLINT NOT NULL,
    wednesday SMALLINT NOT NULL,
    thursday SMALLINT NOT NULL,
    friday SMALLINT NOT NULL,
    saturday SMALLINT NOT NULL,
    sunday SMALLINT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    PRIMARY KEY (agency_id, service_id)
);

CREATE TABLE IF NOT EXISTS calendar_dates (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    service_id TEXT NOT NULL,
    date DATE NOT NULL,
    exception_type SMALLINT NOT NULL,
    PRIMARY KEY (agency_id, service_id, date)
);

CREATE TABLE IF NOT EXISTS routes (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    route_id TEXT NOT NULL,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type SMALLINT,
    route_url TEXT,
    route_color TEXT,
    route_text_color TEXT,
    route_desc TEXT,
    route_desc_detail TEXT,
    PRIMARY KEY (agency_id, route_id)
);

CREATE TABLE IF NOT EXISTS route_patterns (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    route_pattern_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    direction_id SMALLINT,
    route_pattern_typicality SMALLINT,
    PRIMARY KEY (agency_id, route_pattern_id),
    FOREIGN KEY (agency_id, route_id) REFERENCES routes(agency_id, route_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS directions (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    route_direction_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    direction_id SMALLINT,
    direction TEXT,
    direction_legacy TEXT,
    PRIMARY KEY (agency_id, route_direction_id),
    FOREIGN KEY (agency_id, route_id) REFERENCES routes(agency_id, route_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS shapes (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    shape_id TEXT NOT NULL,
    shape_pt_lat DOUBLE PRECISION NOT NULL,
    shape_pt_lon DOUBLE PRECISION NOT NULL,
    shape_pt_sequence INTEGER NOT NULL,
    route_pattern_id TEXT,
    PRIMARY KEY (agency_id, shape_id, shape_pt_sequence)
);

-- Stores connected shape points into a line for a particular shape_id
CREATE TABLE IF NOT EXISTS shape_geoms (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    shape_id TEXT NOT NULL,
    geom geometry(LineString, 4326) NOT NULL,
    length_m DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (agency_id, shape_id)
);

CREATE TABLE IF NOT EXISTS stops (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    stop_id TEXT NOT NULL,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    stop_url TEXT,
    location_type SMALLINT,
    parent_station TEXT,
    wheelchair_boarding SMALLINT,
    PRIMARY KEY (agency_id, stop_id)
);

CREATE TABLE IF NOT EXISTS trips (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    trip_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_headsign TEXT,
    direction_id SMALLINT,
    shape_id TEXT,
    wheelchair_accessible SMALLINT,
    route_pattern_id TEXT,
    PRIMARY KEY (agency_id, trip_id),
    FOREIGN KEY (agency_id, route_id) REFERENCES routes(agency_id, route_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stop_times (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    trip_id TEXT NOT NULL,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    pickup_type SMALLINT,
    PRIMARY KEY (agency_id, trip_id, stop_sequence),
    FOREIGN KEY (agency_id, trip_id) REFERENCES trips(agency_id, trip_id) ON DELETE CASCADE,
    FOREIGN KEY (agency_id, stop_id) REFERENCES stops(agency_id, stop_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS translations (
    agency_id TEXT NOT NULL REFERENCES agency(agency_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    language TEXT NOT NULL,
    record_id TEXT NOT NULL,
    translation TEXT,
    PRIMARY KEY (agency_id, table_name, field_name, language, record_id)
);
