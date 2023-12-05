import time
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv
from influxdb import InfluxDBClient
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer

load_dotenv()

dbhost = os.getenv('DB_HOST', 'localhost')
dbport = int(os.getenv('DB_PORT'))
dbuser = os.getenv('DB_USERNAME')
dbpassword = os.getenv('DB_PASSWORD')
dbname = os.getenv('DB_DATABASE')
topic = os.getenv('TOPIC')
kafka = os.getenv('KAFKA_URL')

def storeMetric(processedEntires, predictionsNumber, validPredictions):
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    measurementData = [
        {
            "measurement": topic,
            "time": timestamp,
            "fields": {
                "processedEntires": processedEntires,
                "predictionsNumber": predictionsNumber,
                "validPredictions": validPredictions
            }
        }
    ]
    influxDBConnection.write_points(measurementData, time_precision='ms')

def generatePredictions(df, epoch, model):
    columns = ['start_station_id', 'end_station_id']
    for column in columns:
        df = df.withColumn(column, F.col(column).cast(FloatType()))
    df = df.withColumn('label', F.col('duration').cast(FloatType()))
    vectorAssembler = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid('skip')
    assembled = vectorAssembler.transform(df)
    prediction = model.transform(assembled)
    prediction.select('prediction', 'label', 'features').show(truncate=False)
    predictionsNumber = prediction.count()
    validPredictions = prediction.filter((prediction['prediction'] - prediction['label']).between(-50, 50)).count()
    storeMetric(df.count(), predictionsNumber, validPredictions)

if __name__ == '__main__':
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

    MODEL = os.getenv('MODEL_PATH')
    influxDBConnection = InfluxDBClient(dbhost, dbport, dbuser, dbpassword, dbname)

    spark = (
        SparkSession.builder.appName("project3-classification")
            .getOrCreate()
    )

    model = PipelineModel.load(MODEL)
    spark.sparkContext.setLogLevel("INFO")
    sampleDataframe = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
    ).selectExpr("CAST(value as STRING)", "timestamp").select(
        from_json(col("value"), dataSchema).alias("sample"), "timestamp"
    ).select("sample.*")

    sampleDataframe.writeStream \
        .foreachBatch(lambda df, epoch_id: generatePredictions(df, epoch_id, model)) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start().awaitTermination()
