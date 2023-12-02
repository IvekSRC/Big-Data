from kafka import KafkaProducer
import csv
import json
import time

if __name__ == '__main__':
    msgProducer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                value_serializer=lambda x: x.encode('utf-8'), api_version=(0, 11, 5))
    print('Kafka Producer has been initiated')

    with open('oslo-bikes.csv') as csvFile:
        data = csv.DictReader(csvFile)
        for row in data:
            row['started_at'] = str(row['started_at'])
            row['ended_at'] = str(row['ended_at'])
            row['duration'] = int(row['duration'])
            row['start_station_id'] = str(row['start_station_id'])
            row['start_station_name'] = str(row['start_station_name'])
            row['start_station_description'] = str(row['start_station_description'])
            row['start_station_latitude'] = float(row['start_station_latitude'])
            row['start_station_longitude'] = float(row['start_station_longitude'])
            row['end_station_id'] = str(row['end_station_id'])
            row['end_station_name'] = str(row['end_station_name'])
            row['end_station_description'] = str(row['end_station_description'])
            row['end_station_latitude'] = float(row['end_station_latitude'])
            row['end_station_longitude'] = float(row['end_station_longitude'])

            print(json.dumps(row))
            msgProducer.send('flink', json.dumps(row))
            msgProducer.flush()

            time.sleep(0.1)

    print('Kafka message producer done!')
