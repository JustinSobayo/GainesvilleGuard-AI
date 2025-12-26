import json
import time
import os
# TODO: Install kafka-python or confluent-kafka
# from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'raw_crime_data'

def get_producer():
    """
    TODO: Initialize and return a Kafka Producer from the library of your choice.
    """
    pass

def fetch_data_from_source():
    """
    TODO: Implement logic to fetch real-time or recent crime data from the Gainesville API.
    Return a list of dictionaries or a single dictionary.
    """
    # Example: return requests.get(url).json()
    pass

def run():
    """
    TODO: Main loop.
    1. Fetch data.
    2. Serialize to JSON.
    3. Send to Kafka topic.
    4. Sleep for interval.
    """
    print("Producer skeleton running... (Implement logic here)")
    
    # producer = get_producer()
    # while True:
    #     data = fetch_data_from_source()
    #     producer.send(TOPIC_NAME, value=json.dumps(data).encode('utf-8'))
    #     time.sleep(10)

if __name__ == "__main__":
    run()
