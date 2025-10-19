from typing import Callable, TypeAlias, Any, Optional
from confluent_kafka import (
    Consumer as KafkaConsumer, 
    Producer as KafkaProducer,
    KafkaError,
    Message
)
from source.broker.connections import KafkaConnectionConfig
from source.broker.interfaces import MessageInterface


DeliveryCallback: TypeAlias = Callable[[KafkaError | None, Message | None], None]


class InterProcessGateway:

    def __init__(self, config: KafkaConnectionConfig) -> None:
        self.config = config
        self.consumer = None
        self.producer = None
        self._init_consumer()
        self._init_producer()

    def _init_consumer(self):
        if self.config.consumer_config is None:
            return
        self.consumer = KafkaConsumer(self.config.consumer_config.as_dict())

    def _init_producer(self):
        if self.config.producer_config is None:
            return
        self.producer = KafkaProducer(self.config.producer_config.as_dict())
    
    def send(
        self, 
        model: MessageInterface, 
        topic: str, 
        payload: Any,
        key: Optional[str] = None,
        cb: Optional[DeliveryCallback] = None
    ) -> None:
        if self.producer is None:
            raise ValueError('Producer is not initialized')
        self.producer.produce(
            topic=topic,
            key=key.encode() if isinstance(key, str) else key,
            value=model.serialize(payload),
            on_delivery=cb
        )

    def _close_producer(self):
        remaining_messages = self.producer.flush(timeout=10)
        if remaining_messages != 0:
            # Warning: some messages were not delivered and were lost
            pass 

    def close(self):
        if self.producer is not None:
            self._close_producer()
        