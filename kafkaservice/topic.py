import os
from kafka.admin import KafkaAdminClient, NewTopic

env_kafka = os.environ.get("KAFKA_URI")
KAFKA_URL = env_kafka if env_kafka else "localhost:9092"

def new_topic(topic):
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_URL 
    )
    existing_topics = admin_client.list_topics()

    # Check if the topic already exists
    if topic in existing_topics:
        print(f"Topic '{topic}' already exists. Skipping creation.")
        return {"message": f"Topic '{topic}' already exists."}, 200
        
    topic_list = []
    topic_list.append(NewTopic(topic, 1, 1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    return {"message": f"Topic '{topic}' created successfully."}, 201
