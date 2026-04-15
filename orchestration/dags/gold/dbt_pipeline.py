from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

import sys
sys.path.insert(0, "/opt/airflow/config")
from config.settings import settings

log = logging.getLogger(__name__)


# Constantes
DBT_IMAGE = "data-lakehouse-dbt:latest"  
DOCKER_NETWORK = "data-lakehouse_lakehouse-network"
PROJ_DIR = settings.PROJECT_ROOT


# Profiles + project ficam no volume montado em /usr/app/dbt
DBT_PROFILES_DIR = "/usr/app/dbt"
DBT_PROJECT_DIR = "/usr/app/dbt"


# Default args  (mesmo padrão do restante do projeto)
DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes = 5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes = 20),
    "execution_timeout": timedelta(hours = 1),
    "sla": timedelta(minutes = 45),
}


# Mount: expõe o projeto dbt local dentro do container (hot-reload)
DBT_MOUNTS = [
    Mount(
        target=DBT_PROJECT_DIR,
        source=f"{PROJ_DIR}/dbt",
        type="bind",
    ),
]

# Variáveis de ambiente repassadas ao container dbt
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "TRINO_HOST": getattr(settings, "TRINO_HOST",       "trino"),
    "TRINO_PORT": str(getattr(settings, "TRINO_PORT",   "8080")),
    "TRINO_USER": getattr(settings, "TRINO_USER",       "admin"),
    "TRINO_CATALOG": getattr(settings, "TRINO_CATALOG",    "iceberg"),
    "TRINO_SCHEMA": getattr(settings, "TRINO_SCHEMA",     "gold"),
}


# Callbacks
def _on_failure(context: dict) -> None:
    ti = context["task_instance"]
    log.error(
        "Falha | dag=%s | task=%s | run_id=%s | try=%s",
        ti.dag_id, ti.task_id, ti.run_id, ti.try_number,
    )


def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.warning(
        "SLA miss | dag=%s | tasks bloqueadas=%s",
        dag.dag_id, [t.task_id for t in blocking_tis],
    )



# Helpers
def _build_dbt_command() -> str:
    """
    Monta o comando dbt levando em conta a Airflow Variable opcional 'dbt_select'.

    Exemplos de Variable:
        dbt_select = "dim_movies"          - dbt run --select dim_movies
        dbt_select = "tag:gold"            - dbt run --select tag:gold
        dbt_select = ""  (ou não definida) - dbt run  (todos os modelos)
    """
    select: str = Variable.get("dbt_select", default_var = "").strip()

    base = (
        f"run "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--project-dir  {DBT_PROJECT_DIR}"
    )

    if select:
        base += f" --select {select}"

    return base


# DAG
with DAG(
    dag_id = "dbt_gold",
    description = "Executa os modelos dbt (camada Gold) via DockerOperator sobre o Trino.",
    schedule_interval = "0 4 * * *",   # 04:00 UTC
    start_date = datetime(2026, 1, 1),
    catchup = False,
    max_active_runs = 1,
    default_args = DEFAULT_ARGS,
    on_failure_callback = _on_failure,
    sla_miss_callback = _on_sla_miss,
    tags = ["gold", "dbt", "batch", "trino"],
    doc_md = __doc__,
) as dag:

    start = EmptyOperator(
        task_id = "start"
    )

    run_dbt = DockerOperator(
        task_id = "run_dbt",
        image = DBT_IMAGE,
        container_name = "airflow__dbt_gold_run__{{ ts_nodash }}",

        # dbt já tem ENTRYPOINT ["dbt"] no Dockerfile,
        # portanto só passamos os sub-argumentos.
        command = _build_dbt_command(),

        environment = DBT_ENV,
        mounts = DBT_MOUNTS,
        working_dir = DBT_PROJECT_DIR,
        network_mode = DOCKER_NETWORK,
        docker_url = "unix://var/run/docker.sock",

        auto_remove = "success",
        mount_tmp_dir = False,
        retrieve_output = True,
        force_pull = False,
        tty = True,
        xcom_all = False,
        do_xcom_push = False,

        api_version = "auto",
        docker_conn_id = None,
        skip_on_exit_code = None,

        doc_md=(
            "Executa `dbt run` dentro do container dbt. "
            "Filtre modelos via Airflow Variable **dbt_select** (ex: `tag:gold`, `dim_movies`). "
            "Exit 0 = sucesso; qualquer outro valor = FAILED + retry automático."
        ),
    )

    end = EmptyOperator(
        task_id = "end",
        trigger_rule = TriggerRule.ALL_DONE,
    )

    start >> run_dbt >> end