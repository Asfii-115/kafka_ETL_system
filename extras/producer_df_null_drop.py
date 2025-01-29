from sqlalchemy import create_engine
from confluent_kafka import Producer
import pandas as pd
import json

# Kafka Configuration
kafka_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(kafka_config)
topic = 'mysql-data-topic'

# MySQL Database Configuration
db_url = "mysql+pymysql://app_user:Test_Pass%40123@localhost:3306/my_db"  # Replace with your credentials
engine = create_engine(db_url)

def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def extract_data():
    """Query data from MySQL."""
    query = "SELECT * FROM ledgers"  # Replace 'ledgers' with your actual table name
    df = pd.read_sql(query, engine)
    
    # Drop columns with only null values
    df = df.dropna(axis=1, how='all')
    print("Data extracted from MySQL after dropping null columns:")
    print(df)
    return df

def produce_to_kafka(df):
    """Send rows of the dataframe to Kafka."""
    for _, row in df.iterrows():
        message = row.dropna().to_json()  # Drop NaN values for individual rows
        producer.produce(topic, value=message, callback=delivery_report)
        producer.flush()

if __name__ == "__main__":
    # Extract data from MySQL
    data = extract_data()
    
    # Send data to Kafka
    produce_to_kafka(data)
    print("Data successfully sent to Kafka!")
