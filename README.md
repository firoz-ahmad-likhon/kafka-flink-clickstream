# Kafka Flink Clickstream

## Problem
Clickstream events have a few practical issues for real-time analytics:

- User activity is spread across many page views, clicks, and session events.
- Session-level behavior cannot be understood from a single event.
- Batch processing delays visibility into user journeys, engagement, and conversion trends.

This becomes a real business problem when teams need live metrics such as active sessions, page views, click-through rates, and conversion funnels.

## Solution

This project uses a simple real-time streaming pattern:

- Kafka streams clickstream events in real time.
- Flink groups related events and maintains journey context with stateful processing.
- Flink enriches events, calculates metrics, and emits results.

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
