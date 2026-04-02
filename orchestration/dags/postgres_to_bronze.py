from __future__ import annotations

import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task

logger = logging.getLogger(__name__)

# =========================
# CONFIG
# =========================
DAG_ID = "postgres_to_bronze"
POSTGRES_CONN_ID = "postgres_lakehouse"

SPARK_CONTAINER = "spark"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
INGEST_SCRIPT = "/opt/spark/app/processing/spark/jobs/ingest_postgres.py"

# =========================
# DEFAULT ARGS
# =========================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# =========================
# HELPERS
# =========================
def _get_postgres_conn() -> dict:
    return BaseHook.get_connection(POSTGRES_CONN_ID)

# =========================
# DAG
# =========================
with DAG(
    dag_id = DAG_ID,
    start_date = datetime(2024, 1, 1),
    schedule_interval = "0 2 * * *",
    catchup = False,
    max_active_runs = 1,
    default_args = default_args,
    tags = ["bronze", "ingestion"],
) as dag:

    # =========================
    # DISCOVER TABLES
    # =========================
    @task
    def discover_tables() -> list[str]:
        conn = _get_postgres_conn()

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

    # =========================
    # INGEST TASK (DINÂMICO)
    # =========================
    def build_command(table: str) -> str:
        return (
            f"docker exec "
            f"-e TABLE_NAME={table} "
            f"{SPARK_CONTAINER} "
            f"{SPARK_SUBMIT} "
            f"--conf spark.jars.ivy=/tmp/.ivy "
            f"{INGEST_SCRIPT}"
        )

    ingest_tasks = BashOperator.partial(
        task_id = "ingest_table",
        append_env = True,
    ).expand(
        bash_command = tables.map(build_command)
    )

    tables >> ingest_tasks