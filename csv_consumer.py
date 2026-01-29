# Alexy Pichette
# Milestone 1 
# Cloud computing
# 1/28/2026

from google.cloud import pubsub_v1
import json

project_id = "pubsub-ms1"
subscription_id = "ReadCSV-sub"

# Create subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")

def callback(message):
    record = json.loads(message.data.decode("utf-8"))
    print("Consumed record:")
    for key, value in record.items():
        print(f"  {key}: {value}")
    print("-" * 30)
    
    message.ack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
