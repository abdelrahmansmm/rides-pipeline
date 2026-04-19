CREATE DATABASE airflow_meta;

\connect rides_hailing;


CREATE TYPE trip_status AS ENUM
  ('requested','accepted','in_progress','completed','cancelled');

CREATE TYPE vehicle_type AS ENUM
  ('economy','business','motorbike','tuk_tuk');

CREATE TYPE payment_method AS ENUM
  ('cash','wallet','credit_card');


CREATE TABLE zones (
  zone_id SERIAL PRIMARY KEY,
  zone_name VARCHAR(100) NOT NULL,
  governorate VARCHAR(50)  NOT NULL,
  latitude NUMERIC(9,6),
  longitude NUMERIC(9,6),
  surge_factor NUMERIC(4,2) DEFAULT 1.0
);


CREATE TABLE drivers (
  driver_id SERIAL PRIMARY KEY,
  full_name VARCHAR(150) NOT NULL,
  national_id CHAR(14) UNIQUE NOT NULL,
  phone VARCHAR(20) NOT NULL,
  vehicle_type vehicle_type NOT NULL,
  vehicle_plate VARCHAR(20) NOT NULL,
  rating NUMERIC(3,2) DEFAULT 5.0,
  active BOOLEAN DEFAULT TRUE,
  registered_zone INT REFERENCES zones(zone_id),
  joined_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE users (
  user_id SERIAL PRIMARY KEY,
  full_name VARCHAR(150) NOT NULL,
  phone VARCHAR(20) UNIQUE NOT NULL,
  email VARCHAR(200),
  joined_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE trips (
  trip_id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users(user_id),
  driver_id INT REFERENCES drivers(driver_id),
  pickup_zone_id INT REFERENCES zones(zone_id),
  dropoff_zone_id INT REFERENCES zones(zone_id),
  pickup_lat NUMERIC(9,6),
  pickup_long NUMERIC(9,6),
  dropoff_lat NUMERIC(9,6),
  dropoff_long NUMERIC(9,6),
  status trip_status NOT NULL DEFAULT 'requested',
  vehicle_type vehicle_type NOT NULL,
  payment_method payment_method NOT NULL,
  requested_at TIMESTAMP DEFAULT NOW(),
  accepted_at TIMESTAMP,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  base_fare NUMERIC(10,2),
  surge_fare NUMERIC(10,2),
  distance_km NUMERIC(8,2),
  duration_min INT,
  cancellation_reason TEXT
);


CREATE INDEX idx_trips_requested_at ON trips(requested_at);
CREATE INDEX idx_trips_driver ON trips(driver_id);
CREATE INDEX idx_trips_status ON trips(status);


-- ── DRIVER_EVENTS table (CDC log from the app) ───────────
-- Captures every status change: online, on-trip, offline
CREATE TABLE driver_events (
  event_id SERIAL PRIMARY KEY,
  driver_id INT REFERENCES drivers(driver_id),
  event_type VARCHAR(50),
  latitude NUMERIC(9,6),
  longitude NUMERIC(9,6),
  occurred_at TIMESTAMP DEFAULT NOW()
);
