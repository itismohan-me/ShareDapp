from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, max as spark_max
import time
import json

# Initialize Spark Session with proper MySQL JDBC configuration
spark = SparkSession.builder \
    .appName("BatchHashtagTrendAnalysis") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28") \
    .getOrCreate()

# Database properties
db_properties = {
    "user": "spark_user",
    "password": "1234",
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/tweet_analysis"
}

# Start timer
start_time = time.time()

try:
    # Read data from MySQL
    hashtag_counts_df = spark.read \
        .jdbc(url=db_properties["url"],
              table="hashtag_counts",
              properties=db_properties)
    
    # Cache the dataframe for better performance
    hashtag_counts_df.cache()

    print("Total records loaded:", hashtag_counts_df.count())

    # Analysis 1: Top hashtags overall
    top_hashtags = hashtag_counts_df \
        .groupBy("hashtag") \
        .agg(count("*").alias("windows_appeared"), 
             spark_max("count").alias("max_count")) \
        .orderBy(desc("max_count")) \
        .limit(10)

    print("Top 10 hashtags overall:")
    top_hashtags.show(truncate=False)

    # Analysis 2: Most active time windows
    active_windows = hashtag_counts_df \
        .groupBy("window_start", "window_end") \
        .agg(count("*").alias("unique_hashtags"),
             spark_max("count").alias("max_hashtag_count")) \
        .orderBy(desc("unique_hashtags")) \
        .limit(5)

    print("Most active time windows:")
    active_windows.show(truncate=False)

    # Analysis 3: Hashtag trends over time (per window)
    hashtag_trends = hashtag_counts_df \
        .groupBy("window_start", "hashtag") \
        .agg(spark_max("count").alias("count")) \
        .orderBy("window_start", desc("count"))

    print("Hashtag trends sample:")
    hashtag_trends.limit(10).show(truncate=False)

except Exception as e:
    print(f"Error during batch processing: {str(e)}")
finally:
    # End timer and calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Batch processing completed in {execution_time:.2f} seconds")

    # Save batch processing results and execution time
    batch_results = {
        "execution_time": execution_time,
        "total_records": hashtag_counts_df.count() if 'hashtag_counts_df' in locals() else 0,
        "top_hashtag": top_hashtags.first()["hashtag"] if 'top_hashtags' in locals() and top_hashtags.count() > 0 else "None"
    }

    with open("batch_results.json", "w") as f:
        json.dump(batch_results, f)

    print("Results saved to batch_results.json")
    spark.stop()