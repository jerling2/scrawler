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
    JsonMessageModel,
    get_api_command_handshake_extract_jobs_stage_1
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
        config=HandshakeExtractListingsConfig.from_env(),
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
def start_extraction_command():
    return (
        JsonMessageModel(),
        'handshake.command.extract-jobs-stage1',
        get_api_command_handshake_extract_jobs_stage_1()
    )

def test_init_model(extractor, broker, mcp, start_extraction_command):
    consumer_config = extractor.get_ipg_consumer_config()
    broker.set_consumers([consumer_config])
    broker.send(*start_extraction_command)
    mcp.run()
