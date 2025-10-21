from typing import Callable, TypeAlias, Any, Optional
from dataclasses import dataclass
from confluent_kafka import (
    KafkaError,
    Message
)
from source.broker.connections import KafkaConnectionConfig
from source.broker.interfaces import MessageInterface, IPGConsumerConfig


DeliveryCallback: TypeAlias = Callable[[KafkaError | None, Message | None], None]
on_notify: TypeAlias = Callable[[Any], None]


@dataclass(frozen=True)
class EventListener:
    model: MessageInterface
    notify: on_notify


class InterProcessGateway:

    def __init__(self, config: KafkaConnectionConfig) -> None:
        self.config = config
        self.listener_registry = {}
        self.consumer = self.config.get_consumer()
        self.producer = self.config.get_producer()
        self._is_closed = False

    @property
    def is_closed(self):
        return self._is_closed 

    def send(self, model: MessageInterface, topic: str, payload: Any,
        key: Optional[str] = None, cb: Optional[DeliveryCallback] = None
    ) -> None:
        if self.producer is None:
            raise ValueError('Producer is not initialized')
        self.producer.produce(
            topic=topic,
            key=key.encode() if isinstance(key, str) else key,
            value=model.serialize(payload),
            on_delivery=cb
        )

    def set_consumers(self, consumers: list[IPGConsumerConfig]):
        new_listener_registry = {}
        for consumer in consumers:
            event_listener = EventListener(consumer.model, consumer.notify)
            for topic in consumer.topics:
                new_listeners = new_listener_registry.get(topic, [])
                new_listeners.append(event_listener)
                new_listener_registry[topic] = new_listeners
        all_topics = list(new_listener_registry.keys())
        self.consumer.subscribe(all_topics)
        self.listener_registry = new_listener_registry

    def listen(self, timeout: float):
        msg = self.consumer.poll(timeout)
        if msg is None:
            return
        if (error := msg.error()):
            raise Exception(error.str())
        topic = msg.topic()
        for event_listener in self.listener_registry[topic]:
            payload = event_listener.model.deserialize(msg.value())
            event_listener.notify(payload)

    def emit(self):
        self.producer.poll(0)

    def close(self):
    
        def close_producer():
            remaining_messages = self.producer.flush(timeout=10)
            if remaining_messages != 0:
                # Warning: some messages were not delivered and lost.
                pass
        
        def close_consumer():
            self.consumer.close()

        if self.producer is not None:
            close_producer()

        if self.consumer is not None:
            close_consumer()
        
        self._is_closed = True
        