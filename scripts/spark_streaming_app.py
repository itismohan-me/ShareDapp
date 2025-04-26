from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, col, window, count, desc, max as spark_max, 
    lit, current_timestamp, to_timestamp, row_number, expr
)
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, 
    IntegerType, TimestampType
)
from pyspark.sql.functions import rank, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import when
from pyspark.sql.functions import isnan, isnull
from pyspark.sql.functions import sum as spark_sum,approx_count_distinct,first
from pyspark.sql.functions import avg
from pyspark.sql.functions import min as spark_min

import mysql.connector
import json
from kafka import KafkaProducer
from pyspark.sql.window import Window
from datetime import datetime
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeHashtagTrendAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,mysql:mysql-connector-java:8.0.28") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming tweets
schema = StructType([
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("location", StringType()),
    StructField("likes", IntegerType()),
    StructField("retweets", IntegerType())
])

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Database connection details
db_config = {
    "user": "spark_user",
    "password": "1234",
    "host": "localhost",
    "database": "tweet_analysis"
}

def save_to_mysql(df, epoch_id, table_name):
    # Convert timestamp columns to strings before collecting
    timestamp_cols = ['window_start', 'window_end', 'current_timestamp']
    for col_name in timestamp_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast(StringType()))
    
    rows = df.collect()
    if len(rows) == 0:
        return
    
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        if table_name == "hashtag_counts":
            for row in rows:
                query = """
                INSERT INTO hashtag_counts (timestamp, hashtag, count, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE count = %s
                """
                cursor.execute(query, (
                    str(row['current_timestamp']),
                    row['hashtag'],
                    row['count'],
                    str(row['window_start']),
                    str(row['window_end']),
                    row['count']
                ))
                
                producer.send("processed-hashtags", {
                    "timestamp": str(row['current_timestamp']),
                    "hashtag": row['hashtag'],
                    "count": row['count'],
                    "window_start": str(row['window_start']),
                    "window_end": str(row['window_end'])
                })
                
        elif table_name == "window_summary":
            for row in rows:
                query = """
                INSERT INTO window_summary (window_start, window_end, total_tweets, unique_hashtags, most_popular_hashtag)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    total_tweets = %s,
                    unique_hashtags = %s,
                    most_popular_hashtag = %s
                """
                cursor.execute(query, (
                    str(row['window_start']),
                    str(row['window_end']),
                    row['total_tweets'],
                    row['unique_hashtags'],
                    row['most_popular_hashtag'],
                    row['total_tweets'],
                    row['unique_hashtags'],
                    row['most_popular_hashtag']
                ))
                
                producer.send("window-summaries", {
                    "window_start": str(row['window_start']),
                    "window_end": str(row['window_end']),
                    "total_tweets": row['total_tweets'],
                    "unique_hashtags": row['unique_hashtags'],
                    "most_popular_hashtag": row['most_popular_hashtag']
                })
                
        conn.commit()
    except Exception as e:
        print(f"Error saving to MySQL: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Read from Kafka
tweets_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-tweets") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data and apply watermark
parsed_tweets = tweets_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(
        from_json(col("value"), schema).alias("tweet")
    ) \
    .select(
        "tweet.user_id",
        "tweet.timestamp",
        "tweet.content",
        "tweet.hashtags",
        "tweet.location",
        "tweet.likes",
        "tweet.retweets"
    ) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("timestamp", "10 minutes")

# Define window specification
window_duration = "15 minutes"
window_spec = window("timestamp", window_duration)

# Combined processing function
def process_window_stats(df, epoch_id):
    # Process hashtag counts
    hashtag_counts = df \
        .withColumn("hashtag", explode("hashtags")) \
        .groupBy(window_spec, "hashtag") \
        .agg(count("*").alias("count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "hashtag",
            "count",
            current_timestamp().alias("current_timestamp")
        )
    
    # Save hashtag counts
    save_to_mysql(hashtag_counts, epoch_id, "hashtag_counts")
    
    # Process window summary
    window_summary = df \
        .groupBy(window_spec) \
        .agg(
            count("*").alias("total_tweets"),
            approx_count_distinct("hashtags").alias("unique_hashtags")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_tweets",
            "unique_hashtags"
        )
    
    # Find most popular hashtag using Spark SQL first() function
    most_popular = hashtag_counts \
        .groupBy("window_start") \
        .agg(first("hashtag").alias("most_popular_hashtag"))
    
    # Join and save window summary
    full_summary = window_summary \
        .join(most_popular, "window_start", "left") \
        .select(
            col("window_start"),
            col("window_end"),
            "total_tweets",
            "unique_hashtags",
            "most_popular_hashtag"
        )
    
    save_to_mysql(full_summary, epoch_id, "window_summary")

# Main processing stream
query = parsed_tweets \
    .writeStream \
    .foreachBatch(process_window_stats) \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()

# Optional console output for debugging
debug_query = parsed_tweets \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="1 minute") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()