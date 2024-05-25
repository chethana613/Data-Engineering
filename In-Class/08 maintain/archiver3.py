import os
import json
import zlib  # Import zlib for compression
from google.cloud import pubsub_v1, storage
from datetime import datetime
import threading

project_id = "inclass-420802"
subscription_id = "archivetest-sub"
bucket_name = "bucketbatch"

# Initialize the Pub/Sub subscriber and storage client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Batch settings
batch = []
batch_size = 100  # Adjust this value as needed
batch_interval = 10  # Interval to upload batch in seconds

# Lock for thread-safe batch operations
batch_lock = threading.Lock()

def save_to_gcs(batch_data):
    """Save batched data to Google Cloud Storage."""
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
        file_name = f"partd/busdata_{timestamp}.json"
        compressed_data = zlib.compress(json.dumps(batch_data).encode())  # Compress the data
        blob = bucket.blob(file_name)
        blob.upload_from_string(compressed_data, content_type="application/octet-stream")  # Use application/octet-stream as content type for compressed data
        print(f"Saved batch of {len(batch_data)} records to {file_name} in bucket {bucket_name}")
    except Exception as e:
        print(f"An error occurred while saving to GCS: {e}")

def process_batch():
    """Periodically process and upload the batch."""
    global batch
    while True:
        threading.Event().wait(batch_interval)
        with batch_lock:
            if batch:
                save_to_gcs(batch)
                batch = []

def callback(message):
    global batch
    with batch_lock:
        batch.append(json.loads(message.data.decode('utf-8')))
        if len(batch) >= batch_size:
            save_to_gcs(batch)
            batch = []
    message.ack()

def main():
    # Start the batch processing thread
    batch_thread = threading.Thread(target=process_batch)
    batch_thread.daemon = True
    batch_thread.start()

    # Subscribe to Pub/Sub and listen for messages
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    main()
