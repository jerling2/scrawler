import os
import signal
import pytest
from source import (
    HandshakeExtractor1Config,
    HandshakeExtractor1,
    InterProcessGateway,
    KafkaProducerConfig,
    KafkaConsumerConfig,
    KafkaConnectionConfig,
    MongoConnection,
    HandshakeRepoE1,
    MainControlProgram, 
    MCPScrawlerModel,
    HandshakeExtractor1Codec,
    HandshakeTransformer1,
    HandshakeTransformer1Config,
    HandshakeRepoT1,
    HandshakeExtractor2,
    HandshakeExtractor2Config,
    HandshakeRepoE2
)


@pytest.fixture(scope='session')
def broker():
    producer_config = KafkaProducerConfig.from_env(
        client_id="pytest_broker_producer"
    )
    consumer_config = KafkaConsumerConfig.from_env(
        client_id="pytest_broker_consumer",
        group_id="pytest_handshake_p1"
    )
    kafka_connection_config = KafkaConnectionConfig(
        consumer_config=consumer_config,
        producer_config=producer_config
    )
    yield InterProcessGateway(kafka_connection_config)


@pytest.fixture(scope='session')
def mongo_connection():
    conn = MongoConnection()
    conn.connect()
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def r1(mongo_connection):
    r1 = HandshakeRepoE1('pytest_raw_handshake_job_stage1', mongo_connection)
    yield r1
    mongo_connection.get_collection('pytest_raw_handshake_job_stage1').drop()


@pytest.fixture(scope='session')
def r2(mongo_connection):
    r2 = HandshakeRepoT1('pytest_staged_handshake_job_stage1', mongo_connection)
    yield r2
    mongo_connection.get_collection('pytest_raw_handshake_job_stage1').drop()


@pytest.fixture(scope='session')
def repo_e2(mongo_connection):
    repo_e2 = HandshakeRepoE2('pytest_raw_handshake_job_stage2', mongo_connection)
    yield repo_e2
    mongo_connection.get_collection('pytest_raw_handshake_job_stage2').drop()


@pytest.fixture()
def mcp(broker):
    def signal_alarm_handler(signum, frame):
        _ = signum, frame
        os.kill(os.getpid(), signal.SIGINT)
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    mcp = MainControlProgram(MCPScrawlerModel(broker=broker))
    yield mcp
    signal.alarm(0)
    signal.signal(signal.SIGALRM, original_handler)


@pytest.fixture(scope='session')
def e1(broker, r1):
    return HandshakeExtractor1(
        config=HandshakeExtractor1Config(),
        broker=broker,
        repo=r1
    )


@pytest.fixture(scope='session')
def t1(broker, r2):
    return HandshakeTransformer1(
        config=HandshakeTransformer1Config(),
        broker=broker,
        repo=r2
    )


@pytest.fixture(scope='session')
def e2(broker, repo_e2):
    return HandshakeExtractor2(
        config=HandshakeExtractor2Config(),
        broker=broker,
        repo=repo_e2
    )


@pytest.fixture()
def cmd_e1():
    message = HandshakeExtractor1Codec(
        start_page=1,
        end_page=5,
        per_page=50
    )
    return (HandshakeExtractor1Codec, HandshakeExtractor1Codec.TOPIC, message)


def test_pipeline_full(e1, t1, e2, broker, mcp, cmd_e1):
    SECONDS_UNTIL_PREEMPT = 60 #< this is how long the consumers will listen for messages
    broker.set_consumers([e1.consumer_info, t1.consumer_info, e2.consumer_info])
    broker.send(*cmd_e1)
    signal.alarm(SECONDS_UNTIL_PREEMPT)
    mcp.run()
    e2.flush() #< This could be handled in the mcp teardown script.

def test_e2(e2, broker, mcp):
    SECONDS_UNTIL_PREEMPT = 30
    broker.set_consumers([e2.consumer_info])
    signal.alarm(SECONDS_UNTIL_PREEMPT)
    mcp.run()
    e2.flush()