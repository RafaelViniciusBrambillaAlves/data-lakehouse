FROM python:3.10-slim

RUN pip install --no-cache-dir kafka-python python-dotenv

COPY . /opt/project
WORKDIR /opt/project

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ingestion.streaming.kafka.producer"]