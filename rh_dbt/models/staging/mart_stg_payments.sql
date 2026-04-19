WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_payments') }}
)

SELECT
    trip_id,
    transaction_ref,
    provider,
    amount_egp,
    discount_egp,
    net_amount_egp,
    promo_code,
    status          AS payment_status,
    created_at
FROM source
WHERE trip_id IS NOT NULL