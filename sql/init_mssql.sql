CREATE DATABASE MSSQL_DATABASE;
GO

USE MSSQL_DATABASE;
GO

CREATE SCHEMA staging;
GO
CREATE SCHEMA gold;
GO
CREATE SCHEMA dbt_staging;
GO

IF NOT EXISTS (SELECT * FROM sys.tables t 
               JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'staging' AND t.name = 'stg_trips')
CREATE TABLE staging.stg_trips (
    trip_id           INT,
    user_id           INT,
    driver_id         INT,
    pickup_zone_id    INT,
    dropoff_zone_id   INT,
    status            NVARCHAR(50),
    vehicle_type      NVARCHAR(50),
    payment_method    NVARCHAR(50),
    requested_at      DATETIME2,
    completed_at      DATETIME2,
    base_fare         DECIMAL(10,2),
    surge_fare        DECIMAL(10,2),
    distance_km       DECIMAL(8,2),
    duration_min      INT,
    cancellation_reason NVARCHAR(500),
    pickup_zone_name  NVARCHAR(100),
    dropoff_zone_name NVARCHAR(100),
    pickup_governorate NVARCHAR(50),
    _loaded_at        DATETIME2 DEFAULT GETDATE()
);

IF NOT EXISTS (SELECT * FROM sys.tables t 
               JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'staging' AND t.name = 'stg_drivers')
CREATE TABLE staging.stg_drivers (
    driver_id      INT,
    full_name      NVARCHAR(200),
    vehicle_type   NVARCHAR(50),
    vehicle_plate  NVARCHAR(30),
    rating         DECIMAL(3,2),
    active         BIT,
    zone_name      NVARCHAR(100),
    governorate    NVARCHAR(50),
    _loaded_at     DATETIME2 DEFAULT GETDATE()
);

IF NOT EXISTS (SELECT * FROM sys.tables t 
               JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'staging' AND t.name = 'stg_payments')
CREATE TABLE staging.stg_payments (
    trip_id         INT,
    transaction_ref NVARCHAR(100),
    provider        NVARCHAR(50),
    amount_egp      DECIMAL(10,2),
    discount_egp    DECIMAL(10,2),
    net_amount_egp  DECIMAL(10,2),
    promo_code      NVARCHAR(50),
    status          NVARCHAR(20),
    created_at      DATETIME2,
    _loaded_at      DATETIME2 DEFAULT GETDATE()
);


-- Streaming aggregation table (written by PySpark)
IF NOT EXISTS (SELECT * FROM sys.tables t 
               JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'dbo' AND t.name = 'streaming_trip_agg')
CREATE TABLE dbo.streaming_trip_agg (
    window_start       DATETIME2,
    window_end         DATETIME2,
    zone_id            INT,
    trip_count         INT,
    total_revenue_egp  DECIMAL(14,2),
    avg_fare_egp       DECIMAL(10,2),
    avg_distance_km    DECIMAL(8,2),
    processed_at       DATETIME2
);

-- ETL audit log
IF NOT EXISTS (SELECT * FROM sys.tables t 
               JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'dbo' AND t.name = 'etl_run_log')
CREATE TABLE dbo.etl_run_log (
    run_id       INT IDENTITY(1,1) PRIMARY KEY,
    pipeline     NVARCHAR(100),
    started_at   DATETIME2,
    ended_at     DATETIME2,
    rows_loaded  INT,
    status       NVARCHAR(20)  -- 'success' or 'failed'
);
