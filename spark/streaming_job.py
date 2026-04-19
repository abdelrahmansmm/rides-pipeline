# spark/streaming_job.py
# PySpark Structured Streaming: Kafka -> MS SQL Server
# Submit command:
# docker exec -it rp_spark_master /opt/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11 \
#   /opt/spark/work-dir/streaming_job.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, sum as spark_sum,
    to_timestamp, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName('RidesHailing_StreamingPipeline') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Schema for trip.events
TRIP_SCHEMA = StructType([
    StructField('event_type', StringType(), True),
    StructField('trip_id', IntegerType(), True),
    StructField('driver_id', IntegerType(), True),
    StructField('fare_egp', DoubleType(), True),
    StructField('distance_km', DoubleType(), True),
    StructField('zone_id', IntegerType(), True),
    StructField('timestamp', StringType(), True),
])


raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:9092') \
    .option('subscribe', 'trip_events') \
    .option('startingOffsets', 'latest') \
    .option('failOnDataLoss', 'false') \
    .load()

parsed = raw_stream.select(
    from_json(col('value').cast('string'), TRIP_SCHEMA).alias('data'),
    col('timestamp').alias('kafka_ts')
).select('data.*', 'kafka_ts')

parsed = parsed.withColumn('event_time', to_timestamp('timestamp'))

# Groups trip completions into 5-minute intervals for KPI dashboards
agg_stream = parsed \
    .filter(col('event_type') == 'trip_completed') \
    .groupBy(
        window(col('event_time'), '5 minutes'),
        col('zone_id')
    ) \
    .agg(
        count('trip_id').alias('trip_count'),
        spark_sum('fare_egp').alias('total_revenue_egp'),
        avg('fare_egp').alias('avg_fare_egp'),
        avg('distance_km').alias('avg_distance_km')
    ) \
    .select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        'zone_id', 'trip_count', 'total_revenue_egp',
        'avg_fare_egp', 'avg_distance_km',
        current_timestamp().alias('processed_at')
    )

# Write to MS SQL Server
MSSQL_URL = 'jdbc:sqlserver://mssql:1433;databaseName=MSSQL_DATABASE;encrypt=false'
MSSQL_PROPS = {
    'user':   'sa',
    'password': 'your_password_for_mssql',
    'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

def write_to_mssql(batch_df, batch_id):
    """
    Called for each micro-batch by Spark.
    Appends aggregated results to streaming_trip_agg table.
    """
    if not batch_df.isEmpty():
        batch_df.write \
            .jdbc(MSSQL_URL, 'dbo.streaming_trip_agg',
                  mode='append', properties=MSSQL_PROPS)
        print(f'[Batch {batch_id}] Wrote {batch_df.count()} rows to DWH')

query = agg_stream.writeStream \
    .outputMode('update') \
    .foreachBatch(write_to_mssql) \
    .option('checkpointLocation', '/tmp/spark_checkpoint/trip_agg') \
    .trigger(processingTime='60 seconds') \
    .start()

print('Streaming query running. Awaiting termination...')
query.awaitTermination()
