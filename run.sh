#!/bin/bash

# Exit on any command failure
set -e

echo "Starting Real-Time Hashtag Trend Analysis System..."

# Kafka bin path
KAFKA_BIN="/opt/homebrew/opt/kafka/libexec/bin"

# File paths
SPARK_APP="scripts/spark_streaming_app.py"
BATCH_APP="scripts/batch_processing.py"
TWEET_GEN="scripts/tweet_generator.py"
COMPARISON_SCRIPT="scripts/comparison_script.py"

# Create necessary directories
mkdir -p logs output

# Start Zookeeper
echo "Starting Zookeeper..."
$KAFKA_BIN/zookeeper-server-start.sh /opt/homebrew/etc/kafka/zookeeper.properties > logs/zookeeper.log 2>&1 &
ZOOKEEPER_PID=$!
echo "Zookeeper started with PID: $ZOOKEEPER_PID"

# Give Zookeeper some time
sleep 10

# Start Kafka
echo "Starting Kafka..."
$KAFKA_BIN/kafka-server-start.sh /opt/homebrew/etc/kafka/server.properties > logs/kafka.log 2>&1 &
KAFKA_PID=$!
echo "Kafka started with PID: $KAFKA_PID"

# Give Kafka some time
sleep 10

# Create required topics
echo "Verifying Kafka topics..."
for TOPIC in raw-tweets processed-hashtags window-summaries
do
    if ! $KAFKA_BIN/kafka-topics.sh --describe --topic $TOPIC --bootstrap-server localhost:9092 &>/dev/null; then
        echo "Creating topic: $TOPIC"
        $KAFKA_BIN/kafka-topics.sh --create --topic $TOPIC --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    else
        echo "Topic $TOPIC already exists"
    fi
done

# Start tweet generator
echo "Starting tweet generator..."
python3 $TWEET_GEN > logs/tweet_generator.log 2>&1 &
GENERATOR_PID=$!
echo "Tweet generator started with PID: $GENERATOR_PID"

# Start Spark Streaming job
echo "Starting Spark Streaming application..."
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,mysql:mysql-connector-java:8.0.28 \
    $SPARK_APP > logs/spark_streaming.log 2>&1 &
SPARK_PID=$!
echo "Spark Streaming started with PID: $SPARK_PID"

# Run duration (default: 30 minutes)
DURATION=${1:-30}
DURATION_SECONDS=$((DURATION * 60))
END_TIME=$(date -v+${DURATION}M +"%H:%M:%S")

echo "System will run for $DURATION minutes (until $END_TIME)..."
echo "Collection started at: $(date '+%H:%M:%S')"
sleep $DURATION_SECONDS

# Stop generator
echo "Stopping tweet generator..."
kill $GENERATOR_PID
wait $GENERATOR_PID 2>/dev/null || true

# Stop Spark
echo "Stopping Spark Streaming..."
kill $SPARK_PID
wait $SPARK_PID 2>/dev/null || true

# Run batch processing
echo "Running batch processing..."
spark-submit --packages mysql:mysql-connector-java:8.0.28 \
    $BATCH_APP > logs/batch_processing.log 2>&1
echo "Batch processing done."

# Run comparison
echo "Generating comparison..."
python3 $COMPARISON_SCRIPT > logs/comparison.log 2>&1
echo "Comparison complete."

# Display summary
echo "Summary of results:"
if [ -f "output/streaming_vs_batch_comparison.json" ]; then
    echo "Comparison JSON:"
    cat output/streaming_vs_batch_comparison.json
fi

if [ -f "output/comparison_results.png" ]; then
    echo "Visualization image saved to: output/comparison_results.png"
fi

# Ask to stop Kafka/Zookeeper
read -p "Stop Kafka and Zookeeper? (y/n): " STOP_SERVICES
if [[ $STOP_SERVICES == "y" || $STOP_SERVICES == "Y" ]]; then
    echo "Stopping Kafka..."
    kill $KAFKA_PID
    wait $KAFKA_PID 2>/dev/null || true

    echo "Stopping Zookeeper..."
    kill $ZOOKEEPER_PID
    wait $ZOOKEEPER_PID 2>/dev/null || true

    echo "All services stopped."
else
    echo "Kafka and Zookeeper are still running."
    echo "To stop manually, run:"
    echo "kill $KAFKA_PID"
    echo "kill $ZOOKEEPER_PID"
fi

echo "Analysis completed. Check the logs and output folders."
