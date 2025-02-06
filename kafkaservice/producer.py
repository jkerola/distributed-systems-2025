import aiokafka
import json
import os

env_kafka = os.environ.get("KAFKA_URI")
KAFKA_URL = env_kafka if env_kafka else "localhost:9092"

async def send_one(topic, message, bootstrap_servers=KAFKA_URL):
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    try:
        await producer.send_and_wait(topic, message)
        print(f"Sent message: {message}")
    finally:
        await producer.stop()

#example of how to use the producer
""" async def main():
    topic_name = 'example_topic'
    message = {'order_id': 123, 'product': 'widget', 'quantity': 10}
    await send_one(topic_name, message)

asyncio.run(main()) """