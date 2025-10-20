import pytest
from source import (
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaProducerConfig,
    InterProcessGateway,
    JsonMessageModel,
    IPGConsumerConfig
)

@pytest.fixture(scope='session')
def conn_config():
    producer_config = KafkaProducerConfig.from_env(
        client_id="pytest_broker_producer"
    )
    consumer_config = KafkaConsumerConfig.from_env(
        client_id="pytest_broker_consumer",
        group_id="scrawler_pytest"
    )
    kafka_connection = KafkaConnectionConfig(
        consumer_config=consumer_config,
        producer_config=producer_config
    )
    return kafka_connection

@pytest.fixture(scope='session')
def broker(conn_config):
    ipg = InterProcessGateway(conn_config)
    yield ipg
    ipg.close()

@pytest.fixture()
def pytest_payload():
    return \
    {
        'data': 'hello from pytest'
    }

@pytest.fixture()
def producers(pytest_payload):
    def on_deliver(err, msg):
        _ = msg
        if err is not None:
            raise Exception
    topics_01 = ['pytest_topic_1', 'pytest_topic_2']
    topics_02 = ['pytest_topic_2', 'pytest_topic_3']
    num_topics = len(topics_01)
    keys_01 = ['pytest_user_01' for _ in range(num_topics)]
    keys_02 = ['pytest_user_02' for _ in range(num_topics)]
    models = [JsonMessageModel() for _ in range(num_topics)]
    cbs = [on_deliver for _ in range(num_topics)]
    payloads = [pytest_payload for _ in range(num_topics)]
    producer_01 = list(zip(models, topics_01, payloads, keys_01, cbs))
    producer_02 = list(zip(models, topics_02, payloads, keys_02, cbs))
    return [*producer_01, *producer_02]

@pytest.fixture()
def consumers(pytest_payload):
    topics_01 = ['pytest_topic_1', 'pytest_topic_2']
    topics_02 = ['pytest_topic_2', 'pytest_topic_3']
    model = JsonMessageModel()
    def on_notify(payload):
        assert payload == pytest_payload
    consumer_01 = IPGConsumerConfig(topics_01, model, on_notify)
    consumer_02 = IPGConsumerConfig(topics_02, model, on_notify)
    return (consumer_01, consumer_02)

def test_round_trip(broker, consumers, producers):
    RUN_FOR = 30 # Seconds
    for p in producers:
        broker.send(*p)
    broker.set_consumers(consumers)
    for _ in range(RUN_FOR):
        broker.listen(timeout=1.0)
        broker.emit()