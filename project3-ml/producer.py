from kafka import KafkaProducer
import csv
import json
import time
from dotenv import load_dotenv

def main():
    kafka_bootstrap_servers = 'localhost:9092'
    topic = 'project3topic'
    csv_file_path = 'oslo-bikes.csv'

    msgProducer = KafkaProducer(
        bootstrap_servers=[kafka_bootstrap_servers],
        value_serializer=lambda x: x.encode('utf-8'), 
        api_version=(0,11,5)
    )

    with open(csv_file_path) as csvFile:
        data = csv.DictReader(csvFile)
        for row in data:
            msgProducer.send(topic, json.dumps(row))
            msgProducer.flush()

            print('Message: ' + json.dumps(row))
            time.sleep(2)

    print('Messages sent to project3topic have been completed.')

if __name__ == '__main__':
    main()