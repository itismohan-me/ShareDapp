
CREATE DATABASE tweet_analysis;
USE tweet_analysis;


CREATE TABLE hashtag_counts (
    timestamp DATETIME,
    hashtag VARCHAR(255),
    count INT,
    window_start DATETIME,
    window_end DATETIME,
    PRIMARY KEY (timestamp, hashtag)
);

CREATE TABLE window_summary (
    window_start DATETIME,
    window_end DATETIME,
    total_tweets INT,
    unique_hashtags INT,
    most_popular_hashtag VARCHAR(255),
    PRIMARY KEY (window_start)
);


CREATE USER 'spark_user'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON tweet_analysis.* TO 'spark_user'@'localhost';
FLUSH PRIVILEGES;

EXIT;