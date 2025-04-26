#!/bin/bash

# Exit on any error
set -e

echo "Setting up Real-Time Hashtag Trend Analysis Environment..."

# Create required directories
mkdir -p data logs output docs/screenshots

# Check and install Java
if ! command -v java &> /dev/null; then
    echo "Java not found. Installing OpenJDK 11..."
    sudo apt update
    sudo apt install -y openjdk-11-jdk
fi

# Check Java version
java -version

# Check and install MySQL
if ! command -v mysql &> /dev/null; then
    echo "MySQL not found. Installing MySQL..."
    sudo apt update
    sudo apt install -y mysql-server
    sudo service mysql start
    
    # Setup MySQL for the project
    echo "Setting up MySQL database and user..."
    sudo mysql -e "CREATE DATABASE IF NOT EXISTS tweet_analysis;"
    sudo mysql -e "CREATE USER IF NOT EXISTS 'spark_user'@'localhost' IDENTIFIED BY 'password';"
    sudo mysql -e "GRANT ALL PRIVILEGES ON tweet_analysis.* TO 'spark_user'@'localhost';"
    sudo mysql -e "FLUSH PRIVILEGES;"
    
    # Import table schemas
    sudo mysql tweet_analysis < sql/create_tables.sql
fi

# Download and extract Kafka if not already present
if [ ! -d "kafka_2.13-3.4.0" ]; then
    echo "Downloading and extracting Apache Kafka..."
    wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
    tar -xzf kafka_2.13-3.4.0.tgz
    rm kafka_2.13-3.4.0.tgz
fi

# Download and extract Spark if not already present
if [ ! -d "spark-3.3.2-bin-hadoop3" ]; then
    echo "Downloading and extracting Apache Spark..."
    wget https://downloads.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
    tar -xzf spark-3.3.2-bin-hadoop3.tgz
    rm spark-3.3.2-bin-hadoop3.tgz
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Create Kafka topics
echo "Creating Kafka topics..."
# First check if Zookeeper and Kafka are running, if not start them temporarily
if ! nc -z localhost 2181 &>/dev/null; then
    echo "Starting Zookeeper temporarily..."
    kafka_2.13-3.4.0/bin/zookeeper-server-start.sh kafka_2.13-3.4.0/config/zookeeper.properties > logs/zookeeper_setup.log 2>&1 &
    ZOOKEEPER_PID=$!
    sleep 10  # Wait for Zookeeper to start
fi

if ! nc -z localhost 9092 &>/dev/null; then
    echo "Starting Kafka temporarily..."
    kafka_2.13-3.4.0/bin/kafka-server-start.sh kafka_2.13-3.4.0/config/server.properties > logs/kafka_setup.log 2>&1 &
    KAFKA_PID=$!
    sleep 10  # Wait for Kafka to start
fi

# Create topics
kafka_2.13-3.4.0/bin/kafka-topics.sh --create --topic raw-tweets --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
kafka_2.13-3.4.0/bin/kafka-topics.sh --create --topic processed-hashtags --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
kafka_2.13-3.4.0/bin/kafka-topics.sh --create --topic window-summaries --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

# Shut down the temporarily started services
if [ ! -z "$KAFKA_PID" ]; then
    echo "Shutting down temporary Kafka instance..."
    kill $KAFKA_PID
    wait $KAFKA_PID 2>/dev/null || true
fi

if [ ! -z "$ZOOKEEPER_PID" ]; then
    echo "Shutting down temporary Zookeeper instance..."
    kill $ZOOKEEPER_PID
    wait $ZOOKEEPER_PID 2>/dev/null || true
fi

# Make run.sh executable
chmod +x run.sh

echo "Setup completed successfully!"
echo "Run './run.sh' to start the hashtag trend analysis system."