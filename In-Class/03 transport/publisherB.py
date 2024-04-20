from google.cloud import pubsub_v1
import json

project_id = "inclass-420802"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open("bcsample.json", "r") as file:
    data = json.load(file)

v_3406 = [record for record in data["vehicle_3406"]]
v_3257 = [record for record in data["vehicle_3257"]]

for r in v_3406:
    data_str = json.dumps(r)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(future.result())

print(f"Published messages for vehicle 3406 to {topic_path}.")


for r in v_3257:
    data_str = json.dumps(r)
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(future.result())

print(f"Published messages for vehicle 3257 to {topic_path}.")