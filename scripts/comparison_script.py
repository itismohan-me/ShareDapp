import json
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
from datetime import datetime

# Get streaming results from MySQL
def get_streaming_stats():
    conn = mysql.connector.connect(
        host="localhost",
        user="spark_user",
        password="1234",
        database="tweet_analysis"
    )
    cursor = conn.cursor(dictionary=True)
    
    # Get hashtag counts
    cursor.execute("SELECT COUNT(*) as total_records FROM hashtag_counts")
    total_records = cursor.fetchone()["total_records"]
    
    # Get most popular hashtag
    cursor.execute("""
        SELECT hashtag, SUM(count) as total_count 
        FROM hashtag_counts 
        GROUP BY hashtag 
        ORDER BY total_count DESC 
        LIMIT 1
    """)
    result = cursor.fetchone()
    top_hashtag = result["hashtag"] if result else "None"
    
    # Get processing metadata from window_summary
    cursor.execute("SELECT COUNT(*) as window_count FROM window_summary")
    window_count = cursor.fetchone()["window_count"]
    
    cursor.close()
    conn.close()
    
    return {
        "total_records": total_records,
        "top_hashtag": top_hashtag,
        "window_count": window_count
    }

# Load batch results
def get_batch_stats():
    with open("batch_results.json", "r") as f:
        return json.load(f)

# Generate comparison report
def generate_comparison():
    streaming_stats = get_streaming_stats()
    batch_stats = get_batch_stats()
    
    # Compare results
    comparison = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "streaming": {
            "total_records": streaming_stats["total_records"],
            "top_hashtag": streaming_stats["top_hashtag"],
            "window_count": streaming_stats["window_count"]
        },
        "batch": {
            "total_records": batch_stats["total_records"],
            "top_hashtag": batch_stats["top_hashtag"],
            "execution_time": batch_stats["execution_time"]
        },
        "comparison": {
            "same_record_count": streaming_stats["total_records"] == batch_stats["total_records"],
            "same_top_hashtag": streaming_stats["top_hashtag"] == batch_stats["top_hashtag"]
        }
    }
    
    # Save comparison to file
    with open("streaming_vs_batch_comparison.json", "w") as f:
        json.dump(comparison, f, indent=4)
    
    print("Comparison results:")
    print(f"Streaming Records: {streaming_stats['total_records']}")
    print(f"Batch Records: {batch_stats['total_records']}")
    print(f"Streaming Top Hashtag: {streaming_stats['top_hashtag']}")
    print(f"Batch Top Hashtag: {batch_stats['top_hashtag']}")
    print(f"Batch Processing Time: {batch_stats['execution_time']:.2f} seconds")
    
    # Create a simple visualization
    fig, ax = plt.subplots(1, 2, figsize=(12, 5))
    
    # Record comparison
    labels = ['Streaming', 'Batch']
    record_counts = [streaming_stats['total_records'], batch_stats['total_records']]
    ax[0].bar(labels, record_counts)
    ax[0].set_title('Record Count Comparison')
    ax[0].set_ylabel('Number of Records')
    
    # Add execution time
    ax[1].bar(['Batch Processing'], [batch_stats['execution_time']])
    ax[1].set_title('Batch Processing Time')
    ax[1].set_ylabel('Time (seconds)')
    
    plt.tight_layout()
    plt.savefig('comparison_results.png')
    print("Visualization saved as comparison_results.png")

if __name__ == "__main__":
    generate_comparison()