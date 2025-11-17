from dataclasses import dataclass
from source.mcp.interfaces import MCPIterface
from source.broker import (
    InterProcessGateway,
    KafkaConnectionConfig,
    KafkaConsumerConfig,
    KafkaProducerConfig
)


@dataclass(frozen=True)
class MCPScrawlerModel(MCPIterface):
    broker: InterProcessGateway = \
    InterProcessGateway(
        config=KafkaConnectionConfig(
            consumer_config=KafkaConsumerConfig.from_env(
                client_id="scrawler_v1",
                group_id="scrawler"
            ),
            producer_config=KafkaProducerConfig.from_env(
                client_id="scrawler_v1"
            )
        )
    )

    def setup(self):
        pass

    def teardown(self):
        self.broker.close()

    def run_loop(self):
        while not self.broker.is_closed:
            self.broker.listen(timeout=1.0)
            self.broker.emit()