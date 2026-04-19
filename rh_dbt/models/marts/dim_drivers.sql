{{
  config(
    materialized='table'
  )
}}

SELECT
    driver_id,
    full_name,
    vehicle_type,
    vehicle_plate,
    CAST(rating AS DECIMAL(3,2))  AS rating,
    active                         AS is_active,
    zone_name,
    governorate,
    CASE
        WHEN rating >= 4.8 THEN 'Gold'
        WHEN rating >= 4.3 THEN 'Silver'
        ELSE 'Bronze'
    END                            AS driver_tier,
    GETDATE()                      AS _dbt_loaded_at
FROM {{ ref('mart_stg_drivers') }}
WHERE driver_id IS NOT NULL