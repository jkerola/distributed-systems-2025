import asyncio
import os
from flask import Flask, make_response, request
import producer 
import consumer 
import topic

env_kafka = os.environ.get("KAFKA_URI")
KAFKA_URL = env_kafka if env_kafka else "localhost:9092"

app = Flask(__name__)

@app.post("/kafka_topic")
def new_kafka_topic():
    if request.content_type != 'application/json':
        return make_response("Request content type must be JSON", 415)
    
    new_topic = request.json["topic"]
    try:
        response, status_code = topic.new_topic(new_topic)
        return make_response(response, status_code)
    except Exception as e:
        return make_response({"error": str(e)}, 500)

@app.post("/kafka_producer")
def post_message_kafka():
    if request.content_type != 'application/json':
        return make_response("Request content type must be JSON", 415)
    
    data = request.json
    if not data:
        return make_response("Request content type must be JSON", 415)

    topic = data.get("topic")
    message = data.get("message")
    asyncio.run(producer.send_one(topic, message))

    return make_response({"message": "Message sent to Kafka and message is", "result": message})

@app.post("/start_consumer")
def start_consumer():

    if request.content_type != 'application/json':
        return make_response("Request content type must be JSON", 415)
    
    topic = request.json["topic"]

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(consumer.start(topic))
        return make_response({"message": "Consumer started"}), 200
    except KeyError:
        return make_response({"error": "Bad Request"}), 400