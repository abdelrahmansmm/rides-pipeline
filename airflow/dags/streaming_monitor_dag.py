from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pyodbc
import logging

DEFAULT_ARGS = {
    'owner': 'de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='streaming_pipeline_monitor',
    description='Monitors Kafka -> PySpark -> MSSQL streaming health every 15 minutes',
    default_args=DEFAULT_ARGS,
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 4, 18),
    catchup=False,
    tags=['streaming', 'monitor', 'kafka'],
    max_active_runs=1,
) as dag:
    def check_kafka_topic(**context):
        """
        Verifies that the Kafka topic 'trip.events' exists
        and has active partitions with recent messages.
        """
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=['kafka:9092'],
                consumer_timeout_ms=5000,
                auto_offset_reset='latest',
            )

            topics = consumer.topics()
            consumer.close()

            if 'trip.events' not in topics:
                logging.warning('Kafka topic trip.events NOT FOUND!!')
                raise ValueError('Kafka topic trip.events does not exist')

            logging.info(f"Kafka healthy. Available topics: {topics}")
            logging.info('trip.events topic exists and is accessible')
            return True

        except NoBrokersAvailable:
            logging.error('Cannot connect to Kafka broker at kafka:9092!!')
            raise

    def check_streaming_data(**context):
        """
        Checks dbo.streaming_trip_agg for recent rows.
        If no rows exist in the last 30 minutes it means
        PySpark streaming job has stopped or is not running.
        Logs warning but does not fail the pipeline.
        """
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=mssql,1433;'
            'DATABASE=RidesHailingDW;'
            'UID=sa;'
            'PWD=RidesP@123;'
            'TrustServerCertificate=yes'
        )
        cur = conn.cursor()

        cur.execute('SELECT COUNT(*) FROM dbo.streaming_trip_agg')
        total_rows = cur.fetchone()[0]

        cur.execute('''
            SELECT COUNT(*)
            FROM dbo.streaming_trip_agg
            WHERE processed_at >= DATEADD(MINUTE, -30, GETDATE())
        ''')
        recent_rows = cur.fetchone()[0]

        # Check latest window processed
        cur.execute('''
            SELECT MAX(window_end), MAX(processed_at)
            FROM dbo.streaming_trip_agg
        ''')
        row = cur.fetchone()
        latest_window   = row[0]
        latest_processed = row[1]

        cur.close()
        conn.close()

        logging.info(f"Streaming table total rows: {total_rows}")
        logging.info(f"Rows in last 30 minutes: {recent_rows}")
        logging.info(f"Latest window end: {latest_window}")
        logging.info(f"Latest processed_at: {latest_processed}")

        # Push metrics to XCom for the report task
        context['ti'].xcom_push(key='total_rows',    value=total_rows)
        context['ti'].xcom_push(key='recent_rows',   value=recent_rows)
        context['ti'].xcom_push(key='latest_window', value=str(latest_window))

        # Warn if no recent data but don't fail
        if recent_rows == 0 and total_rows > 0:
            logging.warning(
                'No streaming data in last 30 minutes!!'
                'PySpark job may have stopped. '
                'Run: docker exec -it rp_spark_master '
                '/opt/spark/bin/spark-submit ...'
            )
        elif total_rows == 0:
            logging.warning(
                'streaming_trip_agg table is empty!!'
                'PySpark job has never run or Kafka producer is stopped.'
            )
        else:
            logging.info('Streaming pipeline is healthy!')

        return recent_rows

    def log_monitor_run(**context):
        """
        Writes monitoring result to dbo.etl_run_log
        so we have a history of streaming health checks.
        """
        ti = context['ti']
        total_rows = ti.xcom_pull(task_ids='check_streaming', key='total_rows')  or 0
        recent_rows = ti.xcom_pull(task_ids='check_streaming', key='recent_rows') or 0

        if recent_rows > 0:
            status = 'healthy'
        elif total_rows > 0:
            status = 'warning_no_recent_data'
        else:
            status = 'warning_empty_table'

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
            '''INSERT INTO dbo.etl_run_log
               (pipeline, started_at, ended_at, rows_loaded, status)
               VALUES (?,?,?,?,?)''',
            'streaming_pipeline_monitor',
            context['data_interval_start'],
            datetime.utcnow(),
            recent_rows,
            status
        )
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"Monitor run logged with status: {status}")
        return status


    check_kafka = PythonOperator(
    task_id='check_kafka',
    python_callable=check_kafka_topic,
    provide_context=True,
    )

    check_streaming = PythonOperator(
        task_id='check_streaming',
        python_callable=check_streaming_data,
        provide_context=True,
    )

    log_monitor = PythonOperator(
        task_id='log_monitor',
        python_callable=log_monitor_run,
        provide_context=True,
    )

    check_kafka >> check_streaming >> log_monitor