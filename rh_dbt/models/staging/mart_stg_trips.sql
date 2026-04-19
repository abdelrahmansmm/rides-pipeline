WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_trips') }}
)

SELECT
    trip_id,
    user_id,
    driver_id,
    pickup_zone_id,
    dropoff_zone_id,
    LOWER(status)           AS trip_status,
    LOWER(vehicle_type)     AS vehicle_type,
    LOWER(payment_method)   AS payment_method,
    requested_at,
    completed_at,
    base_fare,
    surge_fare,
    distance_km,
    duration_min,
    cancellation_reason,
    pickup_zone_name,
    dropoff_zone_name,
    pickup_governorate,
    CASE
        WHEN base_fare > 0 THEN ROUND(surge_fare / base_fare, 2)
        ELSE 1.0
    END AS surge_multiplier,
    CASE
        WHEN LOWER(status) = 'cancelled' THEN 1
        ELSE 0
    END AS is_cancelled,
    CAST(requested_at AS DATE)      AS trip_date,
    DATEPART(HOUR, requested_at)    AS trip_hour
FROM source
WHERE trip_id IS NOT NULL
  AND base_fare >= 0