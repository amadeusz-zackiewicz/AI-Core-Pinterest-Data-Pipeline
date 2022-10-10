from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaProducer

app = FastAPI()

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send(topic="Pinterest", value=data)
    return item

class Producer(KafkaProducer):
    pass

producer = Producer(
    bootstrap_servers="localhost:9092", 
    client_id="Pinterest Data", 
    value_serializer=lambda mlmessage: json.dumps(mlmessage).encode("ascii")
)

if __name__ == '__main__':
    uvicorn.run("app.kafka_producer:app", host="localhost", port=8000)
