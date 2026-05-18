from __future__ import annotations

from pyflink.datastream import StreamExecutionEnvironment

from flink_job.jobs.clickstream_enrichment.config import parse_config
from flink_job.jobs.clickstream_enrichment.pipeline import build_pipeline, configure_environment


def main(argv: list[str] | None = None) -> None:
    """Run the clickstream enrichment Flink job."""
    config = parse_config(argv)
    stream_env = StreamExecutionEnvironment.get_execution_environment()

    configure_environment(stream_env, config)
    build_pipeline(stream_env, config)

    stream_env.execute(config.job_name)
