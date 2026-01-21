import pytest
from source import KafkaTopicManager, KafkaAdminConfig, KafkaConnectionConfig
from confluent_kafka.admin import NewTopic


@pytest.fixture(scope="session")
def conn():
    conn = KafkaConnectionConfig(
        admin_config=KafkaAdminConfig.from_env()
    ).get_admin()
    return conn

@pytest.fixture()
def pytest_topic():
    return NewTopic(
        topic="pytest.topic.manager",
        num_partitions=1,
        replication_factor=1,
        config={
            'cleanup.policy': 'delete,compact',
            'retention.ms': str(24 * 60 * 60 * 1000), # 24 hours
            'segment.ms': str(12 * 60 * 60 * 1000) # 12 hours
        }
    )

def test_create_topics(conn, pytest_topic):
    manager = KafkaTopicManager(conn)
    manager.create_topics([pytest_topic])

def test_delete_topics(conn):
    targets = ['pytest.topic.manager', 'this.topic.shouldnt.exist']
    manager = KafkaTopicManager(conn)
    manager.delete_topics(targets)

def test_verify_existence(conn, pytest_topic):
    import time
    targets = [pytest_topic.topic]
    manager = KafkaTopicManager(conn)
    manager.create_topics([pytest_topic])
    
    MAX_ATTEMPTS = 5
    attempts = 0
    while not manager.verify_topic_existence(targets) and attempts < MAX_ATTEMPTS:
        print(f"Broker has not created the target topics yet.")
        attempts += 1
        time.sleep(2) # wait two seconds

    if attempts >= MAX_ATTEMPTS:
        raise Exception('failed test')

    manager.delete_topics(targets)
    attempts = 0
    while manager.verify_topic_existence(targets) and attempts < MAX_ATTEMPTS:
        print(f"Broker has not deleted the target topics yet.")
        attempts += 1
        time.sleep(2) # wait two seconds
    
    if attempts >= MAX_ATTEMPTS:
        raise Exception('failed test')

