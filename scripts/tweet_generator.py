import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Sample data for generating tweets
users = ["user" + str(i) for i in range(1, 101)]
hashtags = [
    "tech", "programming", "AI", "datascience", "bigdata", "spark", "kafka",
    "python", "java", "streaming", "analytics", "cloud", "ML", "database",
    "news", "trending", "fashion", "sports", "music", "movies", "food",
    "travel", "health", "fitness", "education", "science", "environment"
]

locations = ["New York", "San Francisco", "Los Angeles", "Chicago", "Seattle", 
             "Boston", "Austin", "Denver", "Miami", "Washington DC"]

# Function to generate a random tweet
def generate_tweet(): 
    user = random.choice(users)
    num_hashtags = random.randint(1, 5)
    tweet_hashtags = random.sample(hashtags, num_hashtags)
    location = random.choice(locations)
    likes = random.randint(0, 1000)
    retweets = random.randint(0, 300)
    
    content_templates = [
        "Just shared my thoughts on {tags}",
        "Can't believe what's happening with {tags}",
        "Everyone should know about {tags}",
        "Amazing developments in {tags}",
        "Here's my take on {tags}",
        "Learning more about {tags} today",
        "Excited to explore {tags}",
        "What do you think about {tags}?",
        "Interesting perspective on {tags}",
        "Breaking news about {tags}"
    ]
    
    content = random.choice(content_templates).format(tags=" ".join(["#" + tag for tag in tweet_hashtags]))
    
    return {
        "user_id": user,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "content": content,
        "hashtags": tweet_hashtags,
        "location": location,
        "likes": likes,
        "retweets": retweets
    }

# Main function to generate tweets and send to Kafka
def main():
    print("Starting tweet generation...")
    tweet_count = 0
    try:
        while True:
            tweet = generate_tweet()
            producer.send('raw-tweets', value=tweet)
            tweet_count += 1
            if tweet_count % 10 == 0:
                print(f"Generated {tweet_count} tweets")
            # Random sleep to simulate realistic tweet flow
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print("Tweet generation stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    main()