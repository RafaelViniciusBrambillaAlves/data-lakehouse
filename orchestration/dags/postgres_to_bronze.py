from __future__ import annotations

import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

# =========================
# CONSTANTES
# =========================
DAG_ID = "postgres_to_bronze"
POSTGRES_CONN_ID = "postgres_lakehouse"
SPARK_CONTAINER = "spark"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
INGEST_SCRIPT = "/opt/spark/app/processing/spark/jobs/ingest_postgres.py"

KNOWN_TABLES = [
    "raw.movies",
    "raw.movie_elicitation_set",
    "raw.belief_data",
]

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
def _get_postgres_conn_params() -> dict:
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    return {
        "host": conn.host,
        "port": conn.port or 5432,
        "dbname": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }

# =========================
# TASKS
# =========================
def discover_tables(**context) -> list[str]:
    """Descobre tabelas no schema raw do PostgreSQL e empurra no XCom."""
    conn_params = _get_postgres_conn_params()

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                  AND table_type   = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]

    if not tables:
        logger.warning("Nenhuma tabela encontrada no schema raw.")
    else:
        logger.info(f"Tabelas descobertas ({len(tables)}): {tables}")

    context["ti"].xcom_push(key = "tables", value = tables)
    return tables


def make_ingest_bash(table: str) -> BashOperator:
    """
    Executa o spark-submit dentro do container 'spark' que já está rodando,
    passando TABLE_NAME como variável de ambiente.
    Credenciais vêm do .env no container spark.
    """
    return BashOperator(
        task_id = f"ingest_{table.replace('.', '_')}",
        bash_command = (
            f"docker exec "
            f"-e TABLE_NAME={table} "
            f"{SPARK_CONTAINER} "
            f"{SPARK_SUBMIT} "
            f"--conf spark.jars.ivy=/tmp/.ivy "
            f"{INGEST_SCRIPT}"
        ),
        # Airflow repassa a saída linha a linha nos logs
        append_env = True,
    )

# =========================
# DAG
# =========================
with DAG(
    dag_id = DAG_ID,
    description = "Ingestão batch PostgreSQL → Bronze (Delta Lake)",
    default_args = default_args,
    start_date = datetime(2024, 1, 1),
    schedule_interval = "0 2 * * *",
    catchup = False,
    max_active_runs = 1,
    tags = ["ingestion", "batch", "bronze", "postgres"],
) as dag:

    discover = PythonOperator(
        task_id         = "discover_tables",
        python_callable = discover_tables,
    )

    ingest_tasks = [make_ingest_bash(table) for table in KNOWN_TABLES]

    discover >> ingest_tasks