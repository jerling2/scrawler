import os
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError


def get_kafka_admin_client() -> AdminClient:
    return AdminClient({
        'bootstrap.servers': os.environ['KAFKA_BOOSTRAP_SERVERS']
    })
    

def create_kafka_topic(admin_client: AdminClient, topics: list[str], num_partitions: int=1) -> None:
    new_topics = [
        NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        for topic in topics
    ]        
    futures = admin_client.create_topics(new_topics)
    for future in futures.values():
        try:
            future.result()
        except KafkaError.TOPIC_ALREAD_EXISTS:
            continue
