from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
from dotenv import load_dotenv

def main():
    load_dotenv()

    spark = SparkSession.builder.appName('project3-training').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    HDFS_DATA = os.getenv('HDFS')
    DATASET = os.getenv('DATASET')
    MODEL = os.getenv('MODEL_PATH')

    dataFrame = spark.read.csv(HDFS_DATA, header=True)

    columns_to_cast = ['start_station_id', 'end_station_id']

    for column in columns_to_cast:
        dataFrame = dataFrame.withColumn(column, F.col(column).cast(FloatType()))

    vector_assembler = VectorAssembler().setInputCols(columns_to_cast).setOutputCol('features').setHandleInvalid('skip')

    assembled = vector_assembler.transform(dataFrame)

    string_indexer = StringIndexer().setInputCol('duration').setOutputCol('label')
    indexedDataFrame = string_indexer.fit(assembled).transform(assembled)

    train_split, test_split = indexedDataFrame.randomSplit([0.8, 0.2], seed=1337)

    print("The model training has started.")

    regressionModel = LogisticRegression(maxIter=100, regParam=0.02, elasticNetParam=0.8)

    pipeline = Pipeline(stages=[regressionModel])
    regressionModelPipe = pipeline.fit(train_split)

    prediction = regressionModelPipe.transform(test_split)

    evaluator = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol='prediction',
                                              metricName='areaUnderPR')
    print("The evaluation has started.")
    accuracy = evaluator.evaluate(prediction)

    print('Accuracy\'s value for logistic regression model is: ' + str(accuracy))

    regressionModelPipe.write().overwrite().save(MODEL)

if __name__ == '__main__':
    main()