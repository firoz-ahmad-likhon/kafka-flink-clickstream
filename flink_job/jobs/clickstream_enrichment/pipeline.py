from __future__ import annotations

from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from flink_job.common.kafka_io import kafka_sink, kafka_source
from flink_job.jobs.clickstream_enrichment.config import ClickstreamJobConfig
from flink_job.jobs.clickstream_enrichment.enrichment import ClickstreamEnrichmentProcessor, user_pseudo_id


def configure_environment(stream_env: StreamExecutionEnvironment, config: ClickstreamJobConfig) -> None:
    """Apply runtime settings to the Flink execution environment."""
    if config.parallelism is not None:
        stream_env.set_parallelism(config.parallelism)

    if config.checkpoint_interval_ms is not None:
        stream_env.enable_checkpointing(config.checkpoint_interval_ms)


def build_pipeline(stream_env: StreamExecutionEnvironment, config: ClickstreamJobConfig) -> None:
    """Build the clickstream enrichment pipeline from runtime configuration."""
    clickstream_events = stream_env.from_source(
        kafka_source(
            topic=config.input_topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.group_id,
            starting_offsets=config.starting_offsets,
        ),
        WatermarkStrategy.no_watermarks(),
        "clickstream-events",
    )

    enriched_events = clickstream_events.key_by(user_pseudo_id).process(ClickstreamEnrichmentProcessor(), output_type=Types.STRING())
    enriched_events.sink_to(kafka_sink(config.output_topic, config.bootstrap_servers)).name("clickstream-events-enriched")
