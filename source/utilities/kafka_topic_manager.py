from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError


class KafkaTopicManager:

    def __init__(self, admin_client: AdminClient) -> None:
        self.admin_client = admin_client

    def get_topics(self) -> list[str]:
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            return list(metadata.topics.keys())
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError._TIMED_OUT:
                print(f"Error: the request to fetch metadata timed out")
            else:
                print(f"Kafka error: {err.str()}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def create_topics(self, topics: list[NewTopic]) -> None:
        existing_topics = self.get_topics()
        filter_new_topics = [t for t in topics if t.topic not in existing_topics]
        if not filter_new_topics:
            return
        fs = self.admin_client.create_topics(filter_new_topics)
        for topic, f in fs.items():
            try:
                f.result()
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")

    def delete_topics(self, targets: list[str]) -> None:
        existing_topics = self.get_topics()
        to_remove = [t for t in targets if t in existing_topics]
        if not to_remove:
            return
        fs = self.admin_client.delete_topics(to_remove)
        for topic, f, in fs.items():
            try:
                f.result()
            except Exception as e:
                print(f"Failed to delete topic '{topic}': {e}")

    def verify_topic_existence(self, topics: list[str]) -> bool:
        existing_topics = self.get_topics()
        for topic in topics:
            if topic not in existing_topics:
                return False
        return True