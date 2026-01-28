import signal
import pytest
import threading
import os
from source import (
    InterProcessGateway,
    MCPHandshakeExtractor1Model,
    MCPHandshakeTransformer1Model,
    MCPHandshakeExtractor2Model,
    MCPHandshakeTransformer2Model,
    MainControlProgram,
    HandshakeExtractor1Codec,
    KafkaAdminConfig,
    KafkaConnectionConfig,
    KafkaProducerConfig,
    get_kafka_admin,
    KafkaTopicManager,
)


def trigger_interrupt():
    os.kill(os.getpid(), signal.SIGINT)

@pytest.fixture()
def timer():
    PREEMPT_S = 45
    return threading.Timer(PREEMPT_S, trigger_interrupt)

@pytest.fixture(scope='session')
def HSE1():
    return MainControlProgram(MCPHandshakeExtractor1Model())


@pytest.fixture(scope='session')
def HST1():
    return MainControlProgram(MCPHandshakeTransformer1Model())


@pytest.fixture(scope='session')
def HSE2():
    return MainControlProgram(MCPHandshakeExtractor2Model())

@pytest.fixture(scope='session')
def HST2():
    return MainControlProgram(MCPHandshakeTransformer2Model())

@pytest.fixture()
def DEV_BROKER():
    kafka_conn = KafkaConnectionConfig(
        producer_config=KafkaProducerConfig.from_env()
    )
    broker = InterProcessGateway(kafka_conn)
    return broker


@pytest.fixture()
def E1_CMD():
    return HandshakeExtractor1Codec(start_page=1, end_page=2, per_page=25)


@pytest.fixture()
def TOPIC_NAMES():
    topics_hse1 = MCPHandshakeExtractor1Model.EXTRACTOR.consumer_info.topics
    topics_hst1 = MCPHandshakeTransformer1Model.TRANSFORMER.consumer_info.topics
    topics_hse2 = MCPHandshakeExtractor2Model.EXTRACTOR.consumer_info.topics
    topics_hst2 = MCPHandshakeTransformer2Model.TRANSFORMER.consumer_info.topics
    return topics_hse1 + topics_hst1 + topics_hse2 + topics_hst2


def test_topic_existence(TOPIC_NAMES):
    client = get_kafka_admin(KafkaAdminConfig.from_env())
    topic_manager = KafkaTopicManager(client)
    assert topic_manager.verify_topic_existence(TOPIC_NAMES)


def test_hse1(DEV_BROKER, HSE1, E1_CMD, timer):
    DEV_BROKER.send(HandshakeExtractor1Codec, HandshakeExtractor1Codec.TOPIC, E1_CMD)
    timer.start()
    try:
        HSE1.run()
    except Exception as e:
        pytest.fail(f'Test failed: {e}')


def test_hst1(HST1, timer):
    timer.start()
    try:
        HST1.run()
    except Exception as e:
        pytest.fail(f'Test failed: {e}')


def test_hse2(HSE2, timer):
    timer.start()
    try:
        HSE2.run()
    except Exception as e:
        pytest.fail(f'Test failed: {e}')

def test_hst2(HST2, timer):
    timer.start()
    try:
        HST2.run()
    except Exception as e:
        pytest.fail(f'Test failed: {e}')
    