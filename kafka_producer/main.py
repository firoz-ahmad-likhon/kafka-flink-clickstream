import logging
import os
import time

from confluent_kafka import Producer

from kafka_producer.publisher import KafkaEventPublisher
from kafka_producer.service import CLICKSTREAM_TOPIC, ClickstreamProducerService

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_KAFKA_ACKS = "all"
DEFAULT_ENABLE_IDEMPOTENCE = "true"
DEFAULT_RETRIES = "2147483647"
DEFAULT_RETRY_BACKOFF_MS = "1000"
DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "5"
DEFAULT_KAFKA_CLIENT_ID = "clickstream-producer"
DEFAULT_PRODUCER_INTERVAL_SECONDS = "30"
DEFAULT_PRODUCER_STARTUP_DELAY_SECONDS = "5"


def main() -> None:
    """Entry."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logger = logging.getLogger(__name__)

    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
            "client.id": os.getenv("KAFKA_CLIENT_ID", DEFAULT_KAFKA_CLIENT_ID),
            "acks": os.getenv("KAFKA_ACKS", DEFAULT_KAFKA_ACKS),
            "enable.idempotence": os.getenv("KAFKA_ENABLE_IDEMPOTENCE", DEFAULT_ENABLE_IDEMPOTENCE).lower() == "true",
            "retries": int(os.getenv("KAFKA_RETRIES", DEFAULT_RETRIES)),
            "retry.backoff.ms": int(os.getenv("KAFKA_RETRY_BACKOFF_MS", DEFAULT_RETRY_BACKOFF_MS)),
            "max.in.flight.requests.per.connection": int(
                os.getenv(
                    "KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION",
                    DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                ),
            ),
        },
    )

    publisher = KafkaEventPublisher(producer=producer, logger=logger)
    clickstream_producer_service = ClickstreamProducerService(publisher=publisher)

    interval_seconds = int(os.getenv("PRODUCER_INTERVAL_SECONDS", DEFAULT_PRODUCER_INTERVAL_SECONDS))
    startup_delay_seconds = int(os.getenv("PRODUCER_STARTUP_DELAY_SECONDS", DEFAULT_PRODUCER_STARTUP_DELAY_SECONDS))

    logger.info("Waiting %s seconds for Kafka startup.", startup_delay_seconds)
    time.sleep(startup_delay_seconds)
    logger.info("Starting clickstream producer for topic=%s interval=%ss", CLICKSTREAM_TOPIC, interval_seconds)

    try:
        while True:
            started = time.monotonic()
            clickstream_producer_service.run_iteration()
            time.sleep(max(0.0, interval_seconds - (time.monotonic() - started)))
    except KeyboardInterrupt:
        logger.info("Clickstream producer stopped.")


if __name__ == "__main__":
    main()
