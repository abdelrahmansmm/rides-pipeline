import psycopg2
import pyodbc
import pandas as pd
from datetime import datetime

log_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"[{log_time}] Starting extraction...")

pg = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="rides_hailing",
    user="postgres_user",
    password="postgres_pass"
)
print("Connected to PostgreSQL")

# Remove the WHERE clause on first run to load all 50k rows
# Add it back after for nightly incremental loads (WHERE t.requested_at >= NOW() - INTERVAL '24 hours')
df_trips = pd.read_sql("""
    SELECT
        t.trip_id,
        t.user_id,
        t.driver_id,
        t.pickup_zone_id,
        t.dropoff_zone_id,
        t.status::text          AS status,
        t.vehicle_type::text    AS vehicle_type,
        t.payment_method::text  AS payment_method,
        t.requested_at,
        t.completed_at,
        t.base_fare,
        t.surge_fare,
        t.distance_km,
        t.duration_min,
        t.cancellation_reason,
        pz.zone_name            AS pickup_zone_name,
        dz.zone_name            AS dropoff_zone_name,
        pz.governorate          AS pickup_governorate
    FROM trips t
    JOIN zones pz ON t.pickup_zone_id  = pz.zone_id
    JOIN zones dz ON t.dropoff_zone_id = dz.zone_id
    ORDER BY t.trip_id
""", pg)

df_drivers = pd.read_sql("""
    SELECT
        d.driver_id,
        d.full_name,
        d.vehicle_type::text AS vehicle_type,
        d.vehicle_plate,
        d.rating,
        d.active,
        z.zone_name,
        z.governorate
    FROM drivers d
    JOIN zones z ON d.registered_zone = z.zone_id
""", pg)

pg.close()
print(f"Extracted {len(df_trips)} trips and {len(df_drivers)} drivers from PostgreSQL")


mssql = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=MSSQL_DATABASE;"
    "UID=sa;"
    "PWD=your_password_for_mssql;"
    "TrustServerCertificate=yes"
)
cur = mssql.cursor()
print("Connected to MSSQL")

print("Loading trips into staging.stg_trips...")
trips_loaded = 0
for _, row in df_trips.iterrows():
    cur.execute("""
        INSERT INTO staging.stg_trips (
            trip_id, user_id, driver_id,
            pickup_zone_id, dropoff_zone_id,
            status, vehicle_type, payment_method,
            requested_at, completed_at,
            base_fare, surge_fare,
            distance_km, duration_min,
            cancellation_reason,
            pickup_zone_name, dropoff_zone_name,
            pickup_governorate)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,
        row.trip_id, row.user_id, row.driver_id,
        row.pickup_zone_id, row.dropoff_zone_id,
        row.status, row.vehicle_type, row.payment_method,
        row.requested_at, row.completed_at,
        row.base_fare, row.surge_fare,
        row.distance_km, row.duration_min,
        row.cancellation_reason,
        row.pickup_zone_name, row.dropoff_zone_name,
        row.pickup_governorate
    )
    trips_loaded += 1

mssql.commit()
print(f"Loaded {trips_loaded} trips")


print("Loading drivers into staging.stg_drivers...")
drivers_loaded = 0
for _, row in df_drivers.iterrows():
    cur.execute("""
        INSERT INTO staging.stg_drivers (
            driver_id, full_name, vehicle_type,
            vehicle_plate, rating, active,
            zone_name, governorate)
        VALUES (?,?,?,?,?,?,?,?)
    """,
        row.driver_id, row.full_name, row.vehicle_type,
        row.vehicle_plate, row.rating, row.active,
        row.zone_name, row.governorate
    )
    drivers_loaded += 1

mssql.commit()
cur.close()
mssql.close()

print(f"Loaded {drivers_loaded} drivers")
print(f"Extraction complete! Total rows: {trips_loaded + drivers_loaded}")
