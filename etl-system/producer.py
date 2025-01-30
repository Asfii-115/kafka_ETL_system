# producer.py
from sqlalchemy import create_engine
from confluent_kafka import Producer
import pandas as pd
import json
import os

# Kafka configuration with environment variables
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
}
producer = Producer(kafka_config)
topic = 'mysql-data-topic'

# MySQL connection with environment variables
mysql_user = os.getenv('MYSQL_USER', 'app_user')
mysql_password = os.getenv('MYSQL_PASSWORD', 'Test_Pass@123')
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_port = os.getenv('MYSQL_PORT', '3306')
mysql_database = os.getenv('MYSQL_DATABASE', 'my_db')

db_url = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"
engine = create_engine(db_url)

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def extract_data():
    query = "SELECT * FROM ledgers"  
    df = pd.read_sql(query, engine)
    print("Data extracted from MySQL:")
    print(df)
    return df

def produce_to_kafka(df):
    for _, row in df.iterrows():
        message = row.to_json()  
        producer.produce(topic, value=message, callback=delivery_report)
        producer.flush()

if __name__ == "__main__":
    data = extract_data()
    produce_to_kafka(data)
    print("Data successfully sent to Kafka!")