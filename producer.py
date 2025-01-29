from sqlalchemy import create_engine
from confluent_kafka import Producer
import pandas as pd
import json


kafka_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(kafka_config)
topic = 'mysql-data-topic'


db_url = "mysql+pymysql://app_user:Test_Pass%40123@localhost:3306/my_db"  
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
