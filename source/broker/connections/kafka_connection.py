from __future__ import annotations
import os
from typing import Optional, Any
from dataclasses import dataclass, field, asdict
from confluent_kafka import (
    Consumer as KafkaConsumer, 
    Producer as KafkaProducer
)
import confluent_kafka


# Refer to the official documentation for the librdkafka library for all, up-to-date, configuration properties.

@dataclass(frozen=True)
class KafkaConnectionConfig:
    consumer_config: Optional[KafkaConsumerConfig] = None
    producer_config: Optional[KafkaProducerConfig] = None

    def get_consumer(self) -> confluent_kafka.Consumer:
        if self.consumer_config is None:
            return
        return KafkaConsumer(self.consumer_config.as_dict())
    
    def get_producer(self) -> confluent_kafka.Producer:
        if self.producer_config is None:
            return
        return KafkaProducer(self.producer_config.as_dict())


@dataclass(frozen=True)
class KafkaConsumerConfig:
    bootstrap_servers: str = field(
        metadata={
            'description': 'Initial list of Kafka brokers (HOST:PORT) the client uses to discover the full cluster', 
            'required': True
        }
    )
    group_id: str = field(
        metadata={
            'description': \
                "A unique string identifying the consumer group this consumer instance belongs to."
                "All consumers with the same group.id cooperate to consume a topic's partitions",
            'required': True
        }
    )
    client_id: Optional[str] = field(
        metadata={
            'description': "An id for this consumer. Useful for logging",
            'example': "my-app-consumer-01"
        },
        default=None
    )
    auto_offset_reset: Optional[str] = field(
        metadata={
            'description': "Refers to where the consumer starts to read messages from the buffer.",
            'example':  \
                "If set to latest, the consumer will only process messages that arrive after it"
                "initializes. If set to earliest, the consumer will process messages that are at"
                "the start of the buffer."
        },
        default=None
    )

    @classmethod
    def from_env(cls, group_id: str, **kwargs) -> KafkaConsumerConfig:
        return KafkaConsumerConfig(
            bootstrap_servers=os.environ['KAFKA_BOOSTRAP_SERVERS'],
            group_id=group_id,
            auto_offset_reset=os.environ['KAFKA_AUTO_OFFSET_RESET'],
            **kwargs
        )

    def as_dict(self) -> dict[str, Any]:
        return \
        {
            key.replace('_', '.'): value 
            for key, value in asdict(self).items() 
            if value is not None
        }


@dataclass(frozen=True)
class KafkaProducerConfig:
    bootstrap_servers: str = field(
        metadata={
            'description': 'Initial list of Kafka brokers (HOST:PORT) the client uses to discover the full cluster', 
            'required': True
        }
    )
    client_id: Optional[str] = field(
        metadata={
            'description': "An id for this producer. Useful for logging",
            'example': "my-app-producer-01"
        },
        default=None
    )

    @classmethod
    def from_env(cls, **kwargs) -> KafkaProducerConfig:
        return KafkaProducerConfig(
            bootstrap_servers=os.environ['KAFKA_BOOSTRAP_SERVERS'],
            **kwargs
        )

    def as_dict(self) -> dict[str, Any]:
        return \
        {
            key.replace('_', '.'): value 
            for key, value in asdict(self).items() 
            if value is not None
        }
