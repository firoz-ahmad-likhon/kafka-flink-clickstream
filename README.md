# Kafka Flink Clickstream

## Problem
Clickstream events have a few practical issues for real-time analytics:

- User activity is spread across many page views, clicks, and session events.
- Session-level behavior cannot be understood from a single event.
- Batch processing delays visibility into user journeys, engagement, and conversion trends.

This becomes a real business problem when teams need live metrics such as active sessions, page views, click-through rates, and conversion funnels.

## Solution

This project uses a simple real-time streaming pattern:

- Kafka streams clickstream events through the `clickstream.events` topic.
- Flink groups related events by user and maintains journey context with stateful processing.
- Flink enriches events with session context and emits results to `clickstream.events.enriched`.

## Prerequisites
- Docker installed.

## Development
1. Clone the repo.
2. Copy the `.env-example` to `.env` and update the values as per your environment.
3. Set `IMAGE_TAG` in `.env` (for example, `IMAGE_TAG=1.0.0`)
4. Start the Docker containers:
   ```
   docker compose up -d --build
   ```

## Flink Job
The Flink job is organized like a production streaming job:

- `flink_job/common/` contains shared helpers that can be reused across jobs.
- `flink_job/jobs/clickstream_enrichment/config.py` parses runtime arguments into a single config object.
- `flink_job/jobs/clickstream_enrichment/main.py` creates the Flink environment, builds the pipeline, and executes the job.
- `flink_job/jobs/clickstream_enrichment/pipeline.py` wires source, enrichment, and sink operators.
- `flink_job/jobs/clickstream_enrichment/enrichment.py` owns keyed state and event enrichment logic.
- `flink_job/clickstream_enrichment.py` remains the compatibility entrypoint used by Docker Compose.

Runtime options are passed as arguments:
```
flink run -d \
  -m flink-jobmanager:8081 \
  -py /opt/flink/usrlib/flink_job/clickstream_enrichment.py \
  --input-topic clickstream.events \
  --output-topic clickstream.events.enriched \
  --bootstrap-servers kafka:29092 \
  --group-id clickstream \
  --starting-offsets earliest \
  --job-name clickstream-events-enrichment \
  --parallelism 1 \
  --checkpoint-interval-ms 10000
```

## Testing

## Type Checking and Linting
This repo uses `pre-commit` hooks to check type and linting before committing the code.

Virtual environment:
```
python -m venv .venv
```
Activate:
```
source .venv/bin/activate
```
In Windows, use:
```
.venv\Scripts\activate
```
Install:
```
pip install pre-commit
pre-commit install
```

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
