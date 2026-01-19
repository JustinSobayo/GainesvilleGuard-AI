"""
Crime Data Producer

Reads historical crime data from S3 and sends it to a Kafka topic
for downstream processing by the crime_processor.

Data Flow: S3 (historical_crime_full.json) --> Kafka (raw_crime_data topic)
"""

import json
import os
from typing import Any

from dotenv import load_dotenv

from kafka import KafkaProducer

from backend.s3_utils import download_data

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'raw_crime_data'
S3_CRIME_DATA_PATH = 'raw/crime/historical_crime_full.json'


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def fetch_crime_data_from_s3() -> list[dict[str, Any]] | None:
    try:
        return download_data(S3_CRIME_DATA_PATH)
    except Exception as e:
        print(f"Error fetching crime data from S3: {str(e)}")
        return None

def send_to_kafka(producer, records: list[dict[str, Any]]) -> int:
    count : int = 0
    try:
        for record in records:
            producer.send(TOPIC_NAME, value=record)
            count += 1
        producer.flush()
        return count
    except Exception as e:
        print(f"Error sending to Kafka: {str(e)}")
        return 0



def run():
    print("Crime Producer starting...")
    print(f"Target Kafka broker: {KAFKA_BROKER}")
    print(f"Target topic: {TOPIC_NAME}")
    print(f"S3 source: {S3_CRIME_DATA_PATH}")

    producer = get_kafka_producer()
    records = fetch_crime_data_from_s3()
    if records:
        count = send_to_kafka(producer, records)
        print(f"Sent {count} crime records to Kafka")
    producer.close()

if __name__ == "__main__":
    run()

