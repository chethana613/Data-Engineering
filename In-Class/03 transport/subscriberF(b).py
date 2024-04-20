from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import sys

# TODO(developer)
project_id = "inclass-420802"

# Number of seconds the subscriber should listen for messages
timeout = 50.0

subscription_id = input("Enter the subscription id: ")

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

file_name = input("Enter the file name to write the result to: ")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with open(file_name, "w") as file:
    original_stdout = sys.stdout
    sys.stdout = file
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            # Wait for messages
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        finally:
            file.close()
            sys.stdout = original_stdout