FROM apache/airflow:2.9.1-python3.11

USER root

# Docker CLI — necessário para o DockerOperator
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Providers necessários
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.9.2 \
    apache-airflow-providers-postgres==5.10.0