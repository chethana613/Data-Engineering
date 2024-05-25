import os
import json
import zlib
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
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

# Generate RSA key pair
key = RSA.generate(2048)
public_key = key.publickey().export_key()

# Initialize RSA cipher
rsa_cipher = PKCS1_OAEP.new(RSA.import_key(public_key))

# Generate AES key
aes_key = get_random_bytes(16)  # 16 bytes key for AES-128

def save_to_gcs(batch_data):
    """Save batched data to Google Cloud Storage."""
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
        file_name = f"parte/busdata_{timestamp}.json"
        
        # Compress the data
        compressed_data = zlib.compress(json.dumps(batch_data).encode())
        
        # Pad the compressed data to a multiple of 16 bytes
        padded_data = pad(compressed_data, AES.block_size)
        
        # Encrypt the padded data using AES
        aes_cipher = AES.new(aes_key, AES.MODE_CBC)
        ct_bytes = aes_cipher.encrypt(padded_data)
        
        # Encrypt the AES key using RSA
        encrypted_aes_key = rsa_cipher.encrypt(aes_key)
        
        # Save encrypted AES key, IV, and encrypted data to GCS
        blob = bucket.blob(file_name)
        blob.upload_from_string(encrypted_aes_key + aes_cipher.iv + ct_bytes, content_type="application/octet-stream")
        
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
