from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import pyodbc
import psycopg2
import pandas as pd
import logging

DEFAULT_ARGS = {
    'owner': 'de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG (
    dag_id='rh_batch_pipeline',
    description='MongoDB + Postgres -> MSSQL staging -> dbt Gold Layer',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 4, 18),
    catchup=False,
    tags=['batch', 'etl', 'ride', 'hailing', 'dbt'],
    max_active_runs=1,
) as dag:
    def extract_mongo_data(**context):
        """
        Reads last 24h payment records from MongoDB and insert into MSSQL staging.
        """
        mongo = MongoClient(
            'mongodb://mongo:27017/',
            serverSelectionTimeoutMS=10000,
            directConnection=True
        )
        col = mongo['payments']['payment_records']

        since = datetime.utcnow() - timedelta(hours=24)
        docs = list(col.find({'created_at': {'$gte': since}}))
        logging.info(f"Fetched {len(docs)} payment records.")

        if not docs:
            logging.info("No new payment records.")
            return 0
        
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=mssql,1433;'
            'DATABASE=RidesHailingDW;'
            'UID=sa;'
            'PWD=RidesP@123;'
            'TrustServerCertificate=yes'
        )
        cur = conn.cursor()
        
        cur.execute('TRUNCATE TABLE staging.stg_payments')

        for doc in docs:
            cur.execute(
                '''
                INSERT INTO staging.stg_payments (trip_id, transaction_ref, provider, amount_egp, discount_egp, net_amount_egp, promo_code, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                doc.get('trip_id'),
                doc.get('transaction_ref'),
                doc.get('provider'),
                doc.get('amount_egp'),
                doc.get('discount_egp'),
                doc.get('net_amount_egp'),
                doc.get('promo_code'),
                doc.get('status'),
                doc.get('created_at')
            )
        
        conn.commit()
        cur.close()
        conn.close()
        mongo.close()

        context['ti'].xcom_push(key='payment_rows', value=len(docs))
        logging.info(f"Inserted {len(docs)} payment records into staging")
        return len(docs)

    def extract_pg_data(**context):
        '''
        Extract trips and drivers data from Postgres and insert into MSSQL staging
        '''
        pg = psycopg2.connect(
            host='postgres',
            port=5432,
            dbname='rides_hailing',
            user='rh_user',
            password='rh_pass'
        )

        df_trips = pd.read_sql('''
            SELECT
                t.trip_id, t.user_id, t.driver_id,
                t.pickup_zone_id, t.dropoff_zone_id,
                t.status::text         AS status,
                t.vehicle_type::text   AS vehicle_type,
                t.payment_method::text AS payment_method,
                t.requested_at, t.completed_at,
                t.base_fare, t.surge_fare,
                t.distance_km, t.duration_min,
                t.cancellation_reason,
                pz.zone_name           AS pickup_zone_name,
                dz.zone_name           AS dropoff_zone_name,
                pz.governorate         AS pickup_governorate
            FROM trips t
            JOIN zones pz ON t.pickup_zone_id  = pz.zone_id
            JOIN zones dz ON t.dropoff_zone_id = dz.zone_id
            WHERE t.requested_at >= NOW() - INTERVAL '24 hours'
            ''', pg)
        
        df_drivers = pd.read_sql('''
            SELECT
                d.driver_id, d.full_name,
                d.vehicle_type::text AS vehicle_type,
                d.vehicle_plate, d.rating, d.active,
                z.zone_name, z.governorate
            FROM drivers d
            JOIN zones z ON d.registered_zone = z.zone_id
            ''', pg)
        
        pg.close()
        logging.info(f"Extracted data of {len(df_trips)} trips and {len(df_drivers)} drivers.")

        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=mssql,1433;'
            'DATABASE=RidesHailingDW;'
            'UID=sa;'
            'PWD=RidesP@123;'
            'TrustServerCertificate=yes'
        )
        cur = conn.cursor()

        cur.execute('TRUNCATE TABLE staging.stg_trips')
        cur.execute('TRUNCATE TABLE staging.stg_drivers')
        conn.commit()

        for _, row in df_trips.iterrows():
            cur.execute(
                '''
                INSERT INTO staging.stg_trips
                   (trip_id, user_id, driver_id,
                    pickup_zone_id, dropoff_zone_id,
                    status, vehicle_type, payment_method,
                    requested_at, completed_at,
                    base_fare, surge_fare,
                    distance_km, duration_min,
                    cancellation_reason,
                    pickup_zone_name, dropoff_zone_name,
                    pickup_governorate)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
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
        
        conn.commit()

        for _, row in df_drivers.iterrows():
            cur.execute(
                '''
                INSERT INTO staging.stg_drivers
                   (driver_id, full_name, vehicle_type,
                    vehicle_plate, rating, active,
                    zone_name, governorate)
                VALUES (?,?,?,?,?,?,?,?)
                ''',
                row.driver_id, row.full_name, row.vehicle_type,
                row.vehicle_plate, row.rating, row.active,
                row.zone_name, row.governorate
            )
        
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"Loaded {len(df_trips)} trips and {len(df_drivers)} drivers")
        context['ti'].xcom_push(key='trip_rows', value=len(df_trips))
        return len(df_trips)

    def log_pipeline_run(**context):
        '''
        Writes a completion record to dbo.etl_run_log.
        Pulls row counts from XCom set by upstream tasks.
        '''

        ti = context['ti']
        payment_rows = ti.xcom_pull(task_ids='extract_payments', key='payment_rows') or 0
        trip_rows = ti.xcom_pull(task_ids='extract_trips_drivers', key='trip_rows') or 0
        total_rows = payment_rows + trip_rows

        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=mssql,1433;'
            'DATABASE=RidesHailingDW;'
            'UID=sa;'
            'PWD=RidesP@123;'
            'TrustServerCertificate=yes'
        )
        cur = conn.cursor()
        cur.execute(
            '''
            INSERT INTO dbo.etl_run_log (pipeline, started_at, ended_at, rows_loaded, status)
            VALUES (?, ?, ?, ?, ?)
            ''',
            'rh_batch_pipeline',
            context['data_interval_start'],
            datetime.utcnow(),
            total_rows,
            'success'
        )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Pipeline logged: {total_rows} total rows.")


    # Tasks
    extract_payments = PythonOperator(
        task_id='extract_payments',
        python_callable=extract_mongo_data,
        provide_context=True,
    )

    extract_trips_drivers = PythonOperator(
        task_id='extract_trips_drivers',
        python_callable=extract_pg_data,
        provide_context=True
    )

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            'cd /opt/airflow/rh_dbt && '
            'dbt run --select staging '
            '--profiles-dir /opt/airflow/rh_dbt '
            '--log-path /tmp/dbt_logs'
        ),
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=(
            'cd /opt/airflow/rh_dbt && '
            'dbt run --select marts '
            '--profiles-dir /opt/airflow/rh_dbt '
            '--log-path /tmp/dbt_logs'
        ),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd /opt/airflow/rh_dbt && '
            'dbt test '
            '--profiles-dir /opt/airflow/rh_dbt '
            '--log-path /tmp/dbt_logs'
        ),
    )

    log_pipeline = PythonOperator(
        task_id='log_pipeline',
        python_callable=log_pipeline_run,
        provide_context=True,
    )

    [extract_payments, extract_trips_drivers] >> dbt_run_staging >> dbt_run_gold >> dbt_test >> log_pipeline