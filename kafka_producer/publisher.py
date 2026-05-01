import json
import logging
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer


class EventPublisher(Protocol):
    """Publisher contract used by the clickstream producer service."""

    def publish(self, topic: str, key: str, payload: dict[str, object]) -> None:
        """Publish one event."""
        ...

    def flush(self) -> None:
        """Flush buffered events."""
        ...


class KafkaEventPublisher:
    """Kafka-backed publisher for clickstream events."""

    def __init__(self, producer: Producer, logger: logging.Logger):
        """Initialize the publisher."""
        self._producer = producer
        self._logger = logger

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        """Log Kafka delivery results."""
        if error is not None:
            self._logger.error("Kafka delivery failed: %s", error)
            return
        self._logger.info("Kafka delivery confirmed topic=%s partition=%s offset=%s", message.topic(), message.partition(), message.offset())

    def publish(self, topic: str, key: str, payload: dict[str, object]) -> None:
        """Serialize and publish one event."""
        self._producer.poll(0)  # Serve queued delivery callbacks without blocking.
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._logger.info("Published event topic=%s key=%s", topic, key)

    def flush(self) -> None:
        """Flush buffered events."""
        pending_messages = self._producer.flush()
        if pending_messages:
            self._logger.warning("Kafka producer flush ended with %s pending message(s)", pending_messages)
