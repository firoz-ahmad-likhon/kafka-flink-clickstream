from dataclasses import dataclass
from datetime import UTC, datetime
from random import choice, random
from uuid import uuid4

from kafka_producer.publisher import EventPublisher

CLICKSTREAM_TOPIC = "clickstream.events"
USER_PSEUDO_IDS = ("user_1", "user_2", "user_3", "user_4", "user_5")
JOURNEY_EVENTS = (
    ("page_view", "https://example.com/", None),
    ("view_item", "https://example.com/products/laptop", None),
    ("click", "https://example.com/products/laptop", "add-to-cart"),
    ("add_to_cart", "https://example.com/cart", "add-to-cart"),
    ("begin_checkout", "https://example.com/checkout", "checkout-button"),
    ("purchase", "https://example.com/order-confirmation", None),
)
JOURNEY_RESET_PROBABILITY = 0.2


@dataclass(slots=True)
class ProducerMessage:
    """Message prepared for Kafka publication."""

    topic: str
    key: str
    payload: dict[str, object]


class ClickstreamProducerService:
    """Generate and publish clickstream events."""

    def __init__(self, publisher: EventPublisher):
        """Initialize producer state."""
        self._publisher = publisher
        self._step_by_user: dict[str, int] = {}

    def run_iteration(self) -> None:
        """Publish one clickstream event."""
        for message in self._build_messages():
            self._publisher.publish(
                topic=message.topic,
                key=message.key,
                payload=message.payload,
            )
        self._publisher.flush()

    def _build_messages(self) -> list[ProducerMessage]:
        """Build the next set of events."""
        user_pseudo_id = choice(USER_PSEUDO_IDS)
        step_index = self._step_by_user.get(user_pseudo_id, 0)
        event_type, page_url, element_id = JOURNEY_EVENTS[step_index]

        payload: dict[str, object] = {
            "event_id": uuid4().hex[:12],
            "event_type": event_type,
            "event_time": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            "user_pseudo_id": user_pseudo_id,
            "page_location": page_url,
            "element_id": element_id,
        }
        self._advance_journey(user_pseudo_id=user_pseudo_id, step_index=step_index)
        return [
            ProducerMessage(
                topic=CLICKSTREAM_TOPIC,
                key=user_pseudo_id,
                payload=payload,
            ),
        ]

    def _advance_journey(self, user_pseudo_id: str, step_index: int) -> None:
        """Advance or reset a user's synthetic journey."""
        next_step = step_index + 1
        if next_step >= len(JOURNEY_EVENTS) or random() < JOURNEY_RESET_PROBABILITY:
            self._step_by_user[user_pseudo_id] = 0
            return
        self._step_by_user[user_pseudo_id] = next_step
