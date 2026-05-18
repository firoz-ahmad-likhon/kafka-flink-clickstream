from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


def user_pseudo_id(raw_event: str) -> str:
    """Extract the Kafka message user id for keyed state."""
    return str(json.loads(raw_event)["user_pseudo_id"])


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO timestamp produced by the clickstream producer."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ClickstreamEnrichmentProcessor(KeyedProcessFunction):
    """Track per-user journey state and emit enriched clickstream events."""

    def open(self, runtime_context: RuntimeContext) -> None:
        """Initialize keyed state for clickstream journey context."""
        self.state = runtime_context.get_state(ValueStateDescriptor("clickstream_state", Types.STRING()))

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context) -> Iterator[str]:
        """Update per-user journey state and emit an enriched metric event."""
        event = json.loads(value)
        current_state = self._load_state()

        event_type = str(event["event_type"])
        event_time = str(event["event_time"])
        is_purchase = event_type == "purchase"

        if event_type == "page_view" or not current_state:
            current_state = {
                "session_started_at": event_time,
                "event_count": 0,
                "purchase_count": 0,
            }

        current_state["event_count"] = int(current_state["event_count"]) + 1
        current_state["last_event_at"] = event_time
        current_state["last_event_type"] = event_type
        if is_purchase:
            current_state["purchase_count"] = int(current_state["purchase_count"]) + 1

        session_duration_seconds = int(
            (parse_timestamp(event_time) - parse_timestamp(str(current_state["session_started_at"]))).total_seconds(),
        )
        enriched_event = {
            "event_id": event["event_id"],
            "user_pseudo_id": event["user_pseudo_id"],
            "event_type": event_type,
            "event_time": event_time,
            "page_location": event["page_location"],
            "element_id": event["element_id"],
            "session_started_at": current_state["session_started_at"],
            "session_event_count": current_state["event_count"],
            "session_duration_seconds": session_duration_seconds,
            "is_conversion": is_purchase,
            "purchase_count": current_state["purchase_count"],
        }

        self.state.update(json.dumps(current_state, separators=(",", ":"), sort_keys=True))
        yield json.dumps(enriched_event, separators=(",", ":"), sort_keys=True)

    def _load_state(self) -> dict[str, Any]:
        state = self.state.value()
        if not state:
            return {}
        return dict(json.loads(state))
