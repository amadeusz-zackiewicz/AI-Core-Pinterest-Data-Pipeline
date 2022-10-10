from kafka import KafkaConsumer
import json
from app.s3 import S3Client

class Consumer(KafkaConsumer):
    pass

data_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

data_consumer.subscribe(topics=["Pinterest"])
aws_s3 = S3Client("^pinterest-data-", "eu-west-2")

print("Subscribed")

for msg in data_consumer:
    #print(f"{msg.timestamp} | {msg.topic} | {msg.value}")
    aws_s3.upload_obj(f"{msg.topic}-{msg.timestamp}.json", msg.value)