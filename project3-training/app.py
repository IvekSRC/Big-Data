from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer
from pyspark.sql.types import FloatType, StructType
import pyspark.sql.functions as F
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import os
from dotenv import load_dotenv

def main():
    load_dotenv()

    spark = SparkSession.builder.appName('project3-training').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    HDFS_DATA = os.getenv('HDFS')
    DATASET = os.getenv('DATASET')
    MODEL = os.getenv('MODEL_PATH')

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

    dataFrame = spark.read.csv(HDFS_DATA, header=True, schema=dataSchema)

    columns_to_cast = ['start_station_id', 'end_station_id']

    for column in columns_to_cast:
        dataFrame = dataFrame.withColumn(column, F.col(column).cast(FloatType()))

    vector_assembler = VectorAssembler().setInputCols(columns_to_cast).setOutputCol('features').setHandleInvalid('skip')

    assembled = vector_assembler.transform(dataFrame)

    string_indexer = StringIndexer().setInputCol('duration').setOutputCol('label')
    indexedDataFrame = string_indexer.fit(assembled).transform(assembled)

    train_split, test_split = indexedDataFrame.randomSplit([0.8, 0.2], seed=1337)

    print("The model training has started.")

    regressionModel = LinearRegression(featuresCol='features', labelCol='label', maxIter=100, regParam=0.02, elasticNetParam=0.8)

    pipeline = Pipeline(stages=[regressionModel])
    regressionModelPipe = pipeline.fit(train_split)

    prediction = regressionModelPipe.transform(test_split)

    evaluator = RegressionEvaluator(labelCol='label', predictionCol='prediction', metricName='rmse')
    rmse = evaluator.evaluate(prediction)
    print("Root Mean Squared Error (RMSE) on test data: %g" % rmse)

    regressionModelPipe.write().overwrite().save(MODEL)

if __name__ == '__main__':
    main()