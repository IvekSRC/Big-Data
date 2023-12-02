from kafka import KafkaProducer
import csv
import json
import time
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    msgProducer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'), api_version=(0,11,5))

    with open('oslo-bikes.csv') as csvFile:
        data = csv.DictReader(csvFile)
        for row in data:
            msgProducer.send('project2Topic', json.dumps(row))
            msgProducer.flush()

            print('Message sent: ' + json.dumps(row))
            time.sleep(2)

    print('The producer sent all the messages!')
