"""
MySQL Database Configuration

This file contains the connection parameters for the MySQL database
used in the Real-Time Hashtag Trend Analysis project.
"""

# MySQL Connection Parameters
config = {
    "host": "localhost",
    "user": "spark_user",
    "password": "1234",
    "database": "tweet_analysis",
    "port": 3306,
    "raise_on_warnings": True,
    "autocommit": True,
    "pool_size": 5,
    "pool_name": "hashtag_analysis_pool",
    "connect_timeout": 10
}

# JDBC URL for Spark
jdbc_url = "jdbc:mysql://localhost:3306/tweet_analysis"

# JDBC Properties for Spark
jdbc_properties = {
    "user": "spark_user", 
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table names
HASHTAG_COUNTS_TABLE = "hashtag_counts"
WINDOW_SUMMARY_TABLE = "window_summary"

# Table creation queries
CREATE_HASHTAG_COUNTS_TABLE = """
CREATE TABLE IF NOT EXISTS hashtag_counts (
    timestamp DATETIME,
    hashtag VARCHAR(255),
    count INT,
    window_start DATETIME,
    window_end DATETIME,
    PRIMARY KEY (timestamp, hashtag)
)
"""

CREATE_WINDOW_SUMMARY_TABLE = """
CREATE TABLE IF NOT EXISTS window_summary (
    window_start DATETIME,
    window_end DATETIME,
    total_tweets INT,
    unique_hashtags INT,
    most_popular_hashtag VARCHAR(255),
    PRIMARY KEY (window_start)
)
"""