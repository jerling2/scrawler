from source.broker.connections import KafkaConnectionConfig


class InterProcessGateway:

    def __init__(self, config: KafkaConnectionConfig) -> None:
        self.config = config
        