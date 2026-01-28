from dataclasses import dataclass
from source.mcp.interfaces import MCPIterface
from source.broker import (
    InterProcessGateway,
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaProducerConfig,
)
from source.services import HandshakeExtractor1


@dataclass(frozen=True)
class MCPHandshakeExtractor1Model(MCPIterface):
    LISTEN_INTERVAL_SECONDS = 1
    BROKER = InterProcessGateway(
        config=KafkaConnectionConfig(
            producer_config=KafkaProducerConfig.from_env(),
            consumer_config=KafkaConsumerConfig.from_env(
                group_id="scrawler_handshake"
            ),
        )
    )
    EXTRACTOR = HandshakeExtractor1(
        broker=BROKER,
    )

    def setup(self):
        self.BROKER.set_consumers([self.EXTRACTOR.consumer_info])
    
    def teardown(self):
        self.BROKER.close()
    
    def run_loop(self):
        while not self.BROKER.is_closed:
            self.BROKER.listen(self.LISTEN_INTERVAL_SECONDS)
            self.BROKER.emit()