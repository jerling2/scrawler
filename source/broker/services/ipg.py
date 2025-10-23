from typing import Callable, TypeAlias, Any, Optional
from dataclasses import dataclass
from confluent_kafka import (
    KafkaError,
    Message
)
from source.broker.connections import KafkaConnectionConfig
from source.broker.interfaces import IPGProtocol, IPGConsumerI


DeliveryCallback: TypeAlias = Callable[[KafkaError | None, Message | None], None]
on_notify: TypeAlias = Callable[[Any], None]


@dataclass(frozen=True)
class EventListener:
    codec: IPGProtocol
    notify: on_notify


class InterProcessGateway:

    def __init__(self, config: KafkaConnectionConfig) -> None:
        self.listener_registry = {}
        self.broker_consumer = config.get_consumer()
        self.broker_producer = config.get_producer()
        self._is_closed = False

    @property
    def is_closed(self):
        return self._is_closed 

    def send(self, codec: IPGProtocol, topic: str, payload: Any,
        key: Optional[str] = None, cb: Optional[DeliveryCallback] = None
    ) -> None:
        if self.broker_producer is None:
            raise ValueError('Producer is not initialized')
        self.broker_producer.produce(
            topic=topic,
            key=key.encode() if isinstance(key, str) else key,
            value=codec.serialize(payload),
            on_delivery=cb
        )

    def set_consumers(self, consumers: list[IPGConsumerI]):
        new_listener_registry = {}
        for consumer in consumers:
            event_listener = EventListener(consumer.codec, consumer.notify)
            for topic in consumer.topics:
                new_listeners = new_listener_registry.get(topic, [])
                new_listeners.append(event_listener)
                new_listener_registry[topic] = new_listeners
        all_topics = list(new_listener_registry.keys())
        self.broker_consumer.subscribe(all_topics)
        self.listener_registry = new_listener_registry

    def listen(self, timeout: float):
        msg = self.broker_consumer.poll(timeout)
        if msg is None:
            return
        if (error := msg.error()):
            raise Exception(error.str())
        topic = msg.topic()
        for event_listener in self.listener_registry[topic]:
            payload = event_listener.codec.deserialize(msg.value())
            event_listener.notify(payload)

    def emit(self):
        self.broker_producer.poll(0)

    def close(self):
    
        def close_producer():
            remaining_messages = self.broker_producer.flush(timeout=10)
            if remaining_messages != 0:
                # Warning: some messages were not delivered and lost.
                pass
        
        def close_consumer():
            self.broker_consumer.close()

        if self.broker_producer is not None:
            close_producer()

        if self.broker_consumer is not None:
            close_consumer()
        
        self._is_closed = True
        