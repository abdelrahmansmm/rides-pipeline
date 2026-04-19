{{
  config(
    materialized='table'
  )
}}

WITH trips AS (
    SELECT * FROM {{ ref('mart_stg_trips') }}
),

payments AS (
    SELECT * FROM {{ ref('mart_stg_payments') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY t.trip_id)  AS fct_trip_sk,
    t.trip_id,
    t.user_id,
    t.driver_id,
    t.pickup_zone_id,
    t.dropoff_zone_id,
    t.trip_status,
    t.vehicle_type,
    t.payment_method,
    t.trip_date,
    t.trip_hour,
    t.pickup_zone_name,
    t.dropoff_zone_name,
    t.pickup_governorate,
    t.base_fare,
    t.surge_fare,
    t.surge_multiplier,
    t.distance_km,
    t.duration_min,
    t.is_cancelled,
    p.net_amount_egp,
    p.provider       AS payment_provider,
    p.promo_code,
    p.discount_egp,
    GETDATE()        AS _dbt_loaded_at
FROM trips t
LEFT JOIN payments p ON t.trip_id = p.trip_id