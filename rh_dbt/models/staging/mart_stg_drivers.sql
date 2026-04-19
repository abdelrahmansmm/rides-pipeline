WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_drivers') }}
)

SELECT
    driver_id,
    full_name,
    LOWER(vehicle_type)  AS vehicle_type,
    vehicle_plate,
    rating,
    active,
    zone_name,
    governorate
FROM source
WHERE driver_id IS NOT NULL