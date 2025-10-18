import pytest
from source import KafkaProducerConfig, KafkaConsumerConfig

@pytest.fixture()
def producer_config():
    return KafkaProducerConfig.from_env(
        client_id='pytest_producer_01'
    )

@pytest.fixture()
def consumer_config():
    return KafkaConsumerConfig.from_env(
        group_id='pytest_consumer_group_01',
        client_id='pytest_consumer_01'
    )
