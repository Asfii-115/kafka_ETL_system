from confluent_kafka import Consumer
import json
import sys

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mysql-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['mysql-data-topic'])


try:
    print("Consuming messages from Kafka...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        # Decode and process the message
        try:
            message = json.loads(msg.value().decode('utf-8'))
            print(f"Received Message: {message}")
            
            # Add your message processing logic here
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    consumer.close()
