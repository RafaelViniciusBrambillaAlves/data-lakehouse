from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

import sys
sys.path.insert(0, "/opt/airflow/config")
from config.settings import settings

logger = logging.getLogger(__name__)


# CONSTANTES
DAG_ID = "postgres_to_bronze"
POSTGRES_CONN_ID = "postgres_lakehouse"

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_IMAGE = "data-lakehouse-spark:latest"
DOCKER_NETWORK = "data-lakehouse_lakehouse-network"
INGEST_SCRIPT = "processing/spark/jobs/ingest_postgres.py"
PROJ_DIR = settings.PROJECT_ROOT


# DEFAULT ARGS
default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
    "email_on_retry": False,
}


# MOUNTS E ENV
SHARED_MOUNTS = [
    Mount(target="/opt/spark/app/processing", source=f"{PROJ_DIR}/processing", type="bind"),
    Mount(target="/opt/spark/app/ingestion", source=f"{PROJ_DIR}/ingestion",  type="bind"),
    Mount(target="/opt/spark/app/config", source=f"{PROJ_DIR}/config",     type="bind"),
    Mount(target="/opt/spark/app/utils", source=f"{PROJ_DIR}/utils",      type="bind"),
    Mount(target="/opt/spark/conf/hive-site.xml", source=f"{PROJ_DIR}/storage/hive/hive-site.xml", type="bind"),
    Mount(target="/opt/spark/app/.env", source=f"{PROJ_DIR}/.env",       type="bind"),
]

SPARK_ENV = {
    "AWS_ACCESS_KEY_ID": settings.AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": settings.AWS_SECRET_KEY,
    "S3_ENDPOINT": settings.S3_ENDPOINT,
    "PYTHONPATH": "/opt/spark/app",
    "SPARK_DRIVER_MEMORY": "4G",
    "SPARK_EXECUTOR_MEMORY": "4G",
    "PYTHONUNBUFFERED": "1",
    "PYTHONIOENCODING": "utf-8",
}


# DAG
with DAG(
    dag_id = DAG_ID,
    start_date = datetime(2024, 1, 1),
    schedule_interval = "0 2 * * *",
    catchup = False,
    max_active_runs = 1,
    default_args = default_args,
    tags = ["bronze", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")

    # DISCOVER TABLES
    @task
    def discover_tables() -> list[str]:
        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        with psycopg2.connect(
            host = conn.host,
            port = conn.port or 5432,
            dbname = conn.schema,
            user = conn.login,
            password = conn.password,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_schema || '.' || table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'raw'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]

        if not tables:
            raise ValueError("Nenhuma tabela encontrada no schema raw")

        logger.info(f"Tabelas encontradas: {tables}")
        return tables

    tables = discover_tables()


    # INGEST — DockerOperator por tabela
    ingest_tasks = DockerOperator.partial(
        task_id = "ingest_table",
        image = SPARK_IMAGE,
        mounts = SHARED_MOUNTS,
        working_dir = "/opt/spark/app",
        network_mode = DOCKER_NETWORK,
        docker_url = "unix://var/run/docker.sock",
        auto_remove = "success",
        mount_tmp_dir = False,
        force_pull = False,
        tty = True,
        do_xcom_push = False,
        api_version = "auto",
    ).expand_kwargs(
        tables.map(lambda table: {
            "container_name": f"airflow__bronze__{table.split('.')[-1]}_{{{{ ts_nodash }}}}",
            "command": (
                f"{SPARK_SUBMIT} "
                "--conf spark.driver.memory=1g "
                "--conf spark.executor.memory=1g "
                "--conf spark.sql.shuffle.partitions=4 "
                f"/opt/spark/app/{INGEST_SCRIPT}"
            ),
            "environment": {**SPARK_ENV, "TABLE_NAME": table},
        })
    )

    end = EmptyOperator(
        task_id = "end",
        trigger_rule = TriggerRule.ALL_DONE,
    )

    start >> tables >> ingest_tasks >> end