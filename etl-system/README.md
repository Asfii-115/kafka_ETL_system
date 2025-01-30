## 📌 Overview
This project creates an ETL (Extract, Transform, Load) pipeline that extracts data from MySQL, publishes it to a Kafka topic, and processes it for storage in Cassandra. It includes schema automation, dynamic data filtering, and robust error handling.

---

## ⚙️ Components

1. **Schema Creation (`create_cassandra_schema.py`)**  
   - Automatically maps MySQL schema to Cassandra data types.  
   - Creates the corresponding table in Cassandra dynamically.  
   - Handles data type conversion and primary key configuration.  

2. **Producer (`producer.py`)**  
   - Extracts data from MySQL using SQLAlchemy.  
   - Drops columns that contain only `NULL` values.  
   - Converts records to JSON format.  
   - Publishes messages to a Kafka topic.  
   - Includes delivery confirmation handling.  

3. **Consumer (`main_consumer.py`)**  
   - Subscribes to the Kafka topic.  
   - Processes incoming messages.  
   - Handles data type conversion for Cassandra.  
   - Inserts filtered data into Cassandra.  
   - Includes error handling and logging.  

4. **Schema Configuration (`schema.json`)**  
   - Defines the Cassandra table structure.  
   - Maps column names to Cassandra data types.  
   - Configures the primary key dynamically.  

---

## 🚀 Setup Instructions

### **1️⃣ Prerequisites**
Ensure you have installed:
- **Python 3.8+**
- **MySQL**
- **Kafka**
- **Cassandra**
- **Confluent Kafka Python Library**
- **SQLAlchemy for MySQL Connectivity**
- **Cassandra Python Driver**

### **2️⃣ Install Dependencies**
```bash
pip install -r requirements.txt
```

### **3️⃣ MySQL Setup**

1. Install MySQL Server if not already installed
2. Create a new database and user:
```sql
CREATE DATABASE my_db;
CREATE USER 'app_user'@'localhost' IDENTIFIED BY '<your_password>';
GRANT ALL PRIVILEGES ON my_db.* TO 'app_user'@'localhost';
FLUSH PRIVILEGES;
```
### 4️⃣ Configure Kafka

1. Download and extract Apache Kafka
2. Start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

3. Start Kafka Server:

```bash
bin/kafka-server-start.sh config/server.properties
```

4. Create the required topic:

```bash
bin/kafka-topics.sh --create --topic mysql-data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
### 5️⃣ Cassandra Setup

1. Install and start Cassandra
2. Create keyspace and set up authentication:

```sql
CREATE KEYSPACE test_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE ROLE IF NOT EXISTS 'cassandra'
WITH PASSWORD = 'cassandra'
AND SUPERUSER = true
AND LOGIN = true;
```
## 🛠 How to Run

1. First, create the Cassandra schema:

```bash
python create_cassandra_schema.py
```

2. Run the producer to start data transfer:

```bash
python producer.py
```

3. Start the Kafka consumer:

```bash
python main_consumer.py
```
## 🔀 Data Flow

1. Producer extracts data from MySQL using SQL query
2. Each row is converted to JSON and published to Kafka topic
3. Consumer receives messages and converts data types according to schema
4. Data is inserted into Cassandra table with appropriate type conversion

## ⛔Error Handling

The system includes error handling for:

- JSON decoding errors
- Database connection issues
- Data type conversion errors
- Message delivery failures
- Invalid data formats

## 📂 Project Structure
```plaintext
📂 kafka-ETL-system
│
├── create_cassandra_schema.py   # Automatically creates table in Cassandra
├── producer.py                  # Extracts MySQL data and sends to Kafka
├── main_consumer.py             # Consumes Kafka messages and inserts into Cassandra
├── schema.json                  # Defines column mapping and primary keys
├── requirements.txt             # List of dependencies
└── README.md                    # Documentation
