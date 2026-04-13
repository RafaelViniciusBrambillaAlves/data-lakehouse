FROM python:3.11-slim
 
WORKDIR /usr/app/dbt
 
# System dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl \
       git \
    && rm -rf /var/lib/apt/lists/*
 
# Pin versions for reproducibility
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-trino==1.8.3
 
# Copy project files at build time so the image is self-contained.
# When running locally, the volume mount in docker-compose overwrites
# this layer, giving you hot-reload without rebuilding.
COPY dbt/ .
 
# Default: run all gold models.
# Override via docker-compose `command:` or `docker run ... dbt <cmd>`.
ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "/usr/app/dbt", "--project-dir", "/usr/app/dbt"]
