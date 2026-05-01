FROM python:3.13-slim

WORKDIR /app

COPY kafka_producer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_producer ./kafka_producer

CMD ["python", "-m", "kafka_producer.main"]
