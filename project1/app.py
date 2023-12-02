import os
from dotenv import load_dotenv
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, mean, stddev, dayofweek
from pyspark.sql.types import StructType

def loadEnvironmentVariables():
    load_dotenv()
    return {
        'APP_NAME': str(os.getenv('APP_NAME')),
        'DATA_SET': os.getenv('DATA_SET'),
        'START_STATION_ID': float(os.getenv('START_STATION_ID')),
        'START_DATE': str(os.getenv('START_DATE')),
        'END_DATE': str(os.getenv('END_DATE')),
        'NUMBER_OF_ROUTES': float(os.getenv('NUMBER_OF_ROUTES')),
    }

def createSparkSession(appName):
    return SparkSession.builder.appName(appName).getOrCreate()

def readData(spark, filePath, schema):
    return spark.read.csv(filePath, schema=schema)

def countStationTrips(dataFrame, startStationId, fromDate, toDate):
    stationTripCount = dataFrame.filter(dataFrame.start_station_id == startStationId) \
        .filter((dataFrame.started_at >= fromDate) & (dataFrame.started_at <= toDate)).count()

    return stationTripCount

def findMostFrequentRoutes(dataFrame, startStationId, fromDate, toDate, numberOfRoutes):
    query = dataFrame.filter(dataFrame.start_station_id == startStationId) \
        .filter((dataFrame.started_at >= fromDate) & (dataFrame.started_at <= toDate)) \
        .groupBy(dataFrame.end_station_name).count()

    mostFrequentRoutes = query.sort(query['count'].desc()).limit(numberOfRoutes).collect()

    return mostFrequentRoutes

def calculateStationStatistics(dataFrame):
    query = dataFrame.groupBy("start_station_name", dayofweek("started_at")).agg(
        min("duration"), max("duration"), stddev("duration"), mean("duration")
    ).collect()

    return query

def main():
    envVariables = loadEnvironmentVariables()
    spark = createSparkSession(envVariables['APP_NAME'])

    dataSchema = StructType() \
        .add("started_at", "timestamp") \
        .add("ended_at", "timestamp") \
        .add("duration", "integer") \
        .add("start_station_id", "integer") \
        .add("start_station_name", "string") \
        .add("start_station_description", "string") \
        .add("start_station_latitude", "decimal") \
        .add("start_station_longitude", "decimal") \
        .add("end_station_id", "integer") \
        .add("end_station_name", "string") \
        .add("end_station_description", "string") \
        .add("end_station_latitude", "decimal") \
        .add("end_station_longitude", "decimal")

    dataFrame = readData(spark, envVariables['DATA_SET'], dataSchema)
    fromDate = datetime.datetime.strptime(envVariables['START_DATE'], "%Y-%m-%dT%H:%M:%S")
    toDate = datetime.datetime.strptime(envVariables['END_DATE'], "%Y-%m-%dT%H:%M:%S")
    startStationId = envVariables['START_STATION_ID']
    numberOfRoutes = int(envVariables['NUMBER_OF_ROUTES'])

    report = []

    stationTripCount = countStationTrips(dataFrame, startStationId, fromDate, toDate)
    report.append(f"Total rides from the selected station during the specified time period is {stationTripCount}\n")

    mostFrequentRoutes = findMostFrequentRoutes(dataFrame, startStationId, fromDate, toDate, numberOfRoutes)
    report.append(f"Top {numberOfRoutes} frequently traveled routes from the chosen origin station within the specified time period:")
    for route in mostFrequentRoutes:
        report.append(f"{route['end_station_name']} [{route['count']}]")

    report.append("\nStatistics group by start station in period of one week")
    stationStatistics = calculateStationStatistics(dataFrame)
    for station in stationStatistics:
        line = (
            f"{station['start_station_name']} on day of the week {station['dayofweek(started_at)']}"
            f" min duration {station['min(duration)']} max duration {station['max(duration)']}"
            f" average duration {station['avg(duration)']} standard deviation of duration {station['stddev_samp(duration)']}"
        )
        report.append(line)

    with open("project1Results.txt", "w") as outputFile:
        outputFile.writelines('\n'.join(report))

    print('Successfully completed process and results can be viewed in project1Result.txt file.')    

if __name__ == '__main__':
    main()
