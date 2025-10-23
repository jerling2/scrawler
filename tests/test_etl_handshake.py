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
    HandshakeExtractor1Codec,
    TransformRawHandshakeJobStage1,
    TransformRawHandshakeJobStage1Config,
    StagedHandshakeJobStage1Repo
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
    r1 = HandshakeRawJobListingsRepo('pytest_raw_handshake_job_stage1', mongo_connection)
    yield r1
    mongo_connection.get_collection('pytest_raw_handshake_job_stage1').drop()


@pytest.fixture(scope='session')
def r2(mongo_connection):
    r2 = StagedHandshakeJobStage1Repo('pytest_staged_handshake_job_stage1', mongo_connection)
    yield r2
    mongo_connection.get_collection('pytest_raw_handshake_job_stage1').drop()


@pytest.fixture()
def mcp(broker):
    SECONDS_UNTIL_PREEMPT = 30 #< An estimate of how long the consumers need to be listening.
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


@pytest.fixture(scope='session')
def e1(broker, r1):
    return HandshakeExtractListings(
        config=HandshakeExtractListingsConfig(),
        broker=broker,
        repo=r1
    )


@pytest.fixture(scope='session')
def t1(broker, r2):
    return TransformRawHandshakeJobStage1(
        config=TransformRawHandshakeJobStage1Config(),
        broker=broker,
        repo=r2
    )

@pytest.fixture()
def cmd_e1():
    message = HandshakeExtractor1Codec(
        start_page=1,
        end_page=5,
        per_page=50
    )
    return (HandshakeExtractor1Codec, HandshakeExtractor1Codec.TOPIC, message)


def test_pipeline_full(e1, t1, broker, mcp, cmd_e1):
    broker.set_consumers([e1.consumer_info, t1.consumer_info])
    broker.send(*cmd_e1)
    mcp.run()

def test_t1(t1, broker, mcp):
    broker.set_consumers([t1.consumer_info])
    mcp.run()
