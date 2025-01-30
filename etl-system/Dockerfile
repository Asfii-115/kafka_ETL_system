# Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY create_cassandra_schema.py .
COPY producer.py .
COPY main_consumer.py .
COPY schema.json .

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV CASSANDRA_HOST=cassandra
ENV MYSQL_HOST=mysql