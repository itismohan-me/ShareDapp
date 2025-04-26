"""
Kafka Configuration

This file contains the configuration parameters for Apache Kafka
used in the Real-Time Hashtag Trend Analysis project.
"""

# Kafka broker settings
BOOTSTRAP_SERVERS = ['localhost:9092']

# Kafka topics
TOPICS = {
    "raw_tweets": "raw-tweets",
    "processed_hashtags": "processed-hashtags",
    "window_summaries": "window-summaries"
}

# Producer configuration
PRODUCER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'client_id': 'hashtag_trend_producer',
    'acks': 'all',  # Wait for all in-sync replicas to acknowledge
    'retries': 3,
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 33554432,  # 32MB buffer
    'key_serializer': None,  # Will be set in the code
    'value_serializer': None  # Will be set in the code
}

# Consumer configuration
CONSUMER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'group_id': 'hashtag_trend_consumers',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5000,  # Commit every 5 seconds
    'session_timeout_ms': 30000,
    'key_deserializer': None,  # Will be set in the code
    'value_deserializer': None  # Will be set in the code
}

# Spark Kafka options
SPARK_KAFKA_OPTIONS = {
    'kafka.bootstrap.servers': ','.join(BOOTSTRAP_SERVERS),
    'subscribe': ','.join(TOPICS.values()),
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false',
    'maxOffsetsPerTrigger': 10000
}

# Zookeeper settings
ZOOKEEPER_HOST = 'localhost:2181'
ZOOKEEPER_TIMEOUT = 10000  # 10 seconds

# Topic configuration
TOPIC_CONFIG = {
    'num_partitions': 3,
    'replication_factor': 1,
    'retention.hours': 24,  # Keep data for 24 hours
    'cleanup.policy': 'delete'
}

# Tweet generation settings
TWEET_RATE = {
    'min_interval': 0.1,  # Minimum time between tweets (seconds)
    'max_interval': 0.5   # Maximum time between tweets (seconds)
}