from dataclasses import dataclass
from source.mcp.interfaces import MCPIterface
from source.broker import (
    InterProcessGateway,
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaProducerConfig,
)
from source.database import HandshakeRepoT2
from source.services import HandshakeTransformer2


@dataclass(frozen=True)
class MCPHandshakeTransformer2Model(MCPIterface):
    LISTEN_INTERVAL_SECONDS = 1
    BROKER = InterProcessGateway(
        config=KafkaConnectionConfig(
            producer_config=KafkaProducerConfig.from_env(),
            consumer_config=KafkaConsumerConfig.from_env(
                group_id="scrawler_handshake"
            ),
        )
    )
    REPO = HandshakeRepoT2('staged.handshake.job.stage2')
    TRANSFORMER = HandshakeTransformer2(
        broker=BROKER,
        repo=REPO,
    )

    def setup(self):
        self.REPO.connect()
        self.BROKER.set_consumers([self.TRANSFORMER.consumer_info])
    
    def teardown(self):
        self.BROKER.close()
    
    def run_loop(self):
        while not self.BROKER.is_closed:
            self.BROKER.listen(self.LISTEN_INTERVAL_SECONDS)
            self.BROKER.emit()