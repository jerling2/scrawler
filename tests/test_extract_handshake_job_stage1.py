import os
import signal
import pytest
from source import (
    HandshakeExtractListingsConfig,
    HandshakeExtractListings,
    InterProcessGateway,
    KafkaProducerConfig,
    KafkaConsumerConfig,
    KafkaConnectionConfig,
    MongoConnection,
    HandshakeRawJobListingsRepo,
    MainControlProgram, 
    MCPScrawlerModel,
    APIExtractHandshakeJobStage1
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
def repo(mongo_connection):
    repo = HandshakeRawJobListingsRepo('pytest_handshake_p1', mongo_connection)
    yield repo
    mongo_connection.get_collection('pytest_handshake_p1').drop()


@pytest.fixture(scope='session')
def extractor(broker, repo):
    yield HandshakeExtractListings(
        config=HandshakeExtractListingsConfig(),
        broker=broker,
        repo=repo
    )

@pytest.fixture(scope='session')
def mcp(broker):
    SECONDS_UNTIL_PREEMPT = 15
    pid = os.getpid()
    def signal_alarm_handler(signum, frame):
        _ = signum, frame
        os.kill(pid, signal.SIGINT)
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    signal.alarm(SECONDS_UNTIL_PREEMPT)
    mcp = MainControlProgram(MCPScrawlerModel(broker=broker))
    yield mcp
    signal.alarm(0)
    signal.signal(signal.SIGALRM, original_handler)

@pytest.fixture()
def cmd():
    cmd = APIExtractHandshakeJobStage1()
    return (cmd.model, cmd.source_topic, cmd.payload)


def test_extract_handshake_job_stage_1(extractor, broker, mcp, cmd):
    broker.set_consumers([extractor.consumer_info])
    broker.send(*cmd)
    mcp.run()
