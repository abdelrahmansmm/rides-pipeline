-- Checking data counts after DAG trigger
SELECT 'stg_trips' AS table_name, COUNT(*) AS row_count
FROM staging.stg_trips
UNION ALL
SELECT 'stg_drivers', COUNT(*)
FROM staging.stg_drivers
UNION ALL
SELECT 'stg_payments', COUNT(*)
FROM staging.stg_payments
UNION ALL
SELECT 'mart_stg_trips', COUNT(*)
FROM dbt_staging.mart_stg_trips
UNION ALL
SELECT 'mart_stg_drivers', COUNT(*)
FROM dbt_staging.mart_stg_drivers
UNION ALL
SELECT 'fct_trips', COUNT(*)
FROM gold.fct_trips
UNION ALL
SELECT 'dim_drivers', COUNT(*)
FROM gold.dim_drivers;

-- Checking data after kafka and spark
SELECT TOP 10 *
FROM dbo.streaming_trip_agg
ORDER BY processed_at DESC;

SELECT COUNT(*)
FROM dbo.streaming_trip_agg;