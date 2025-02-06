import asyncio
import aiokafka
import json
import os

env_kafka = os.environ.get("KAFKA_URI")
KAFKA_URL = env_kafka if env_kafka else "localhost:9092"


async def create_consumer(topic, bootstrap_servers=KAFKA_URL):
    consumer = aiokafka.AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="my-group" ,
        request_timeout_ms=30000
    )
    await consumer.start()
    print(f"Consumer subscribed to topic: {topic}")
    return consumer

async def process_message(message):
    retry_count = 0
    max_retries = 3
    while retry_count < max_retries:
        try:
            print(f"Processing order: {message}")
            await asyncio.sleep(2)  
            print(f"Order {message['order_id']} processed")
            return  
        except Exception as e:
            print(f"Error processing order {message['order_id']}: {e}")
            retry_count += 1
            await asyncio.sleep(2) 
    print(f"Failed to process order {message['order_id']} after {max_retries} retries.")

async def consume_messages(consumer):
    print("Consumer started.")
    try:
        async for message in consumer:
            print(f"Received message: {message.value}")
            await process_message(message.value)
    finally:
        await consumer.stop()

async def start(topic):
    consumer = await create_consumer(topic) 
    await consume_messages(consumer)

