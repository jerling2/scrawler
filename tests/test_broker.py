import pytest
from source import (
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaProducerConfig,
    InterProcessGateway,
    JsonMessageModel
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
def producer_01():
    model = JsonMessageModel()
    topic = 'pytest'
    key = 'pytest_user_01'
    
    def on_deliver(err, msg):
        _ = msg
        if err is not None:
            raise Exception
            
    return (model, topic, key, on_deliver)


def test_broker_init(broker):
    assert broker

def test_producer_01(broker, producer_01):
    model, topic, key, on_deliver = producer_01
    payload = {
        'data': 'Hello from pytest'
    }
    broker.send(
        model,
        topic,
        payload,
        key,
        on_deliver
    )
