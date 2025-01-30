# main_consumer.py
from confluent_kafka import Consumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from decimal import Decimal
import os

with open("schema.json", "r") as schema_file:
    schema = json.load(schema_file)

table_name = schema["table_name"]
columns = schema["columns"]

# Kafka configuration with environment variables
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'mysql-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
consumer.subscribe(['mysql-data-topic'])

# Cassandra configuration with environment variables
cassandra_host = [os.getenv('CASSANDRA_HOST', '127.0.0.1')]
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'test_keyspace')
cassandra_user = os.getenv('CASSANDRA_USER', 'cassandra')
cassandra_password = os.getenv('CASSANDRA_PASSWORD', 'cassandra')

auth_provider = PlainTextAuthProvider(username=cassandra_user, password=cassandra_password)
cluster = Cluster(cassandra_host, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace(cassandra_keyspace)

def convert_value(value, cassandra_type):
    if value is None:
        return None  
    if cassandra_type == "decimal":
        return Decimal(str(value)) if value is not None else None
    elif cassandra_type == "int":
        return int(value) if value is not None else None
    elif cassandra_type == "bigint":
        return int(value) if value is not None else None
    else:  
        return value

try:
    print("Consuming messages from Kafka and storing in Cassandra...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        try:
            message = json.loads(msg.value().decode('utf-8'))
            print(f"Received Message: {message}")
            
            values = []
            for column_name, column_type in columns.items():
                value = message.get(column_name, None)  
                values.append(convert_value(value, column_type))

            column_names = ", ".join(columns.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            session.execute(insert_query, values)
            print(f"Record inserted into Cassandra table {table_name}: {message}")

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Error inserting into Cassandra: {e}")
            print(f"Failed message: {message}")

except KeyboardInterrupt:
    print("Consumption stopped.")
finally:
    consumer.close()
    cluster.shutdown()