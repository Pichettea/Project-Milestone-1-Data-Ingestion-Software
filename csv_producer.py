# Alexy Pichette
# Milestone 1 
# Cloud computing
# 1/28/2026


from google.cloud import pubsub_v1
import csv
import json
import time


project_id = "pubsub-ms1"
topic_name = "ReadCSV"

# Create publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing CSV records to {topic_path}")

# Open CSV file
with open("Labels.csv", newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for row in reader:
        message = json.dumps(row).encode("utf-8")
        
        future = publisher.publish(topic_path, message)
        future.result()
        
        print(f"Published record: {row}")
        time.sleep(0.5)
