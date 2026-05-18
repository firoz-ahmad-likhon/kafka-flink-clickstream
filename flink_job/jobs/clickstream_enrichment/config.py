from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

DEFAULT_INPUT_TOPIC = "clickstream.events"
DEFAULT_OUTPUT_TOPIC = "clickstream.events.enriched"
DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_GROUP_ID = "clickstream"
DEFAULT_JOB_NAME = "clickstream-events-enrichment"


@dataclass(frozen=True, slots=True)
class ClickstreamJobConfig:
    """Runtime configuration for the clickstream enrichment job."""

    input_topic: str
    output_topic: str
    bootstrap_servers: str
    group_id: str
    starting_offsets: str
    job_name: str
    parallelism: int | None
    checkpoint_interval_ms: int | None


def parse_config(argv: list[str] | None = None) -> ClickstreamJobConfig:
    """Parse command line arguments into a job configuration object."""
    parser = argparse.ArgumentParser(description="Run the clickstream enrichment Flink job.")
    parser.add_argument("--input-topic", default=DEFAULT_INPUT_TOPIC, help="Kafka topic containing raw clickstream events.")
    parser.add_argument("--output-topic", default=DEFAULT_OUTPUT_TOPIC, help="Kafka topic for enriched clickstream events.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP_ID), help="Kafka consumer group id.")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest", help="Kafka starting offset policy.")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME, help="Flink job name.")
    parser.add_argument("--parallelism", type=int, default=None, help="Optional Flink job parallelism.")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=None, help="Optional checkpoint interval in milliseconds.")

    args = parser.parse_args(argv)
    return ClickstreamJobConfig(
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        starting_offsets=args.starting_offsets,
        job_name=args.job_name,
        parallelism=args.parallelism,
        checkpoint_interval_ms=args.checkpoint_interval_ms,
    )
