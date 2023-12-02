import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

def processAndStoreMetric(df, epoch, station_id):
    job1 = df
    job2 = df
    
    job1 = job1.filter(
        job1.start_station_id == station_id) \
        .agg(
            min(col("duration").cast('int')).alias('duration_min'), max(col("duration").cast('int')).alias('duration_max'),
            mean(col("duration").cast('int')).alias('duration_avg'),
            count("start_station_id").alias('num_of_rides')
        ).collect()

    job2 = job2.groupBy(
        job2.start_station_name) \
        .agg(count('start_station_name').alias('num_of_rides')).sort(desc('num_of_rides')).take(3)

    if job1[0]['duration_avg']:
        average = job1[0]['duration_avg']
        maximum = job1[0]['duration_max']
        minimum = job1[0]['duration_min']
        rides = job1[0]['num_of_rides']

        cassandra_session.execute(f"""
            INSERT INTO project2Keyspace.duration_metrics(time, avg_duration, max_duration, min_duration, num_of_rides)
            VALUES (toTimeStamp(now()), {average}, {maximum}, {minimum}, {rides})
        """)

    stations = ['Unknown', 'Unknown', 'Unknown']
    rides_number = [-1, -1, -1]
    for i, row in enumerate(job2):
        stations[i] = row['start_station_name']
        rides_number[i] = row['num_of_rides']

    cassandra_session.execute(f"""
        INSERT INTO project2Keyspace.station_metrics(time, station1, rides1, station2, rides2, station3, rides3)
        VALUES (toTimeStamp(now()), '{stations[0]}', {rides_number[0]}, '{stations[1]}', {rides_number[1]}, '{stations[2]}', {rides_number[2]})
    """)

if __name__ == '__main__':
    load_dotenv()

    kafka_url = str(os.getenv('KAFKA_URL'))
    kafka_topic = str(os.getenv('KAFKA_TOPIC'))
    start_station_id = str(os.getenv('START_STATION_ID'))
    cassandra_host = str(os.getenv('CASSANDRA_HOST'))
    process_time = str(os.getenv('PROCESS_TIME'))

    cassandra_cluster = Cluster([cassandra_host], port=9042)
    cassandra_session = cassandra_cluster.connect()

    print('Successfully connected to Cassandra!')

    dataSchema = StructType() \
        .add("started_at", "timestamp") \
        .add("ended_at", "timestamp") \
        .add("duration", "string") \
        .add("start_station_id", "string") \
        .add("start_station_name", "string") \
        .add("start_station_description", "string") \
        .add("start_station_latitude", "decimal") \
        .add("start_station_longitude", "decimal") \
        .add("end_station_id", "string") \
        .add("end_station_name", "string") \
        .add("end_station_description", "string") \
        .add("end_station_latitude", "decimal") \
        .add("end_station_longitude", "decimal")

    spark = (
        SparkSession.builder.appName("project2")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0")
            .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("INFO")
    sampleDataframe = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_url)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
    ).selectExpr("CAST(value as STRING)", "timestamp").select(
        from_json(col("value"), dataSchema).alias("sample"), "timestamp"
    ).select("sample.*")

    sampleDataframe.writeStream \
        .option("spark.cassandra.connection.host", cassandra_host + ':' + str(9042)) \
        .foreachBatch(lambda df, epoch_id: processAndStoreMetric(df, epoch_id, start_station_id)) \
        .outputMode("update") \
        .trigger(processingTime=process_time) \
        .start().awaitTermination()
