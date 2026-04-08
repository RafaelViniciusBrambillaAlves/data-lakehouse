from __future__ import annotations

import logging
import sys 
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

sys.path.insert(0, "/opt/airflow/config")
from config.settings import settings

log = logging.getLogger(__name__)


# CONSTANTS
DAG_ID = "silver_movies"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_JOB = "/opt/spark/app/processing/spark/jobs/movies/main.py"

SPARK_IMAGE = "data-lakehouse-spark:latest"

DOCKER_NETWORK = "data-lakehouse_lakehouse-network"

AWS_CONN_ID = "aws_minio"

PROJ_DIR = settings.PROJECT_ROOT


# DEFAULT ARGS
default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes = 5), # tempo entre retries
    "retry_exponential_backoff": True, # retry exponencial: 5min - 10min - 20min
    "max_retry_delay": timedelta(minutes = 20),
    "execution_timeout": timedelta(hours = 2),
    "sla": timedelta(minutes = 30) # dispara callback se a task não concluir em 30 min
}


# SLA / FAILURE CALLBACKS
def _on_failure(context: dict) -> None:
    ti = context["task_instance"]
  
    log.error(
        "Falha | dag=%s | task=%s | run_id=%s | try=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
       
    )

# monitoramento de atraso
def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.warning(
        "SLA miss | dag=%s | tasks bloqueadas=%s",
        dag.dag_id,
        [t.task_id for t in blocking_tis],

    )


# DAG
with DAG(
    dag_id = DAG_ID,
    description = "Pipeline Bronze -> Silver: movies_cleaned e movie_genres",
    schedule_interval = "0 3 * * *", 
    start_date = datetime(2026, 1, 1),
    catchup = False,
    max_active_runs = 1,
    default_args = default_args,
    on_failure_callback = _on_failure,
    sla_miss_callback = _on_sla_miss,
    tags = ["silver", "movies", "batch", "spark"],
    doc_md = __doc__,
) as dag:

    start = EmptyOperator(
        task_id = "start"
    )

    # controle de dependência de dados
    check_bronze = S3KeySensor(
        task_id = "check_bronze", 
        bucket_name = "bronze",
        bucket_key = "batch/movies/*",
        wildcard_match = True,
        aws_conn_id = AWS_CONN_ID,
        poke_interval = 60, # checa a cada 1 min
        timeout = 1800, 
        mode = "reschedule", # libera worker enquanto espera
        soft_fail = True, # se não achar dado - não quebra a DAG
        doc_md = (
            "Verifica se ha objetos em s3://bronze/batch/movies/. "
            "Se o bronze estiver vazio após 30 min, a run é pulada silenciosamente."
        )
    )

    run_spark = DockerOperator(
        task_id = "run_spark_pipeline",
        image = SPARK_IMAGE,
        container_name = f"airflow__{DAG_ID}__{{{{ ts_nodash }}}}",
        command = (
            f"{SPARK_SUBMIT} "
            "--conf spark.jars.ivy=/tmp/.ivy "
            f"{SPARK_JOB}"
        ), # execução isolada
        environment = {
            "AWS_ACCESS_KEY_ID": settings.AWS_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": settings.AWS_SECRET_KEY,
            "S3_ENDPOINT": settings.S3_ENDPOINT,
            "PYTHONPATH": "/opt/spark/app",
            "SPARK_DRIVER_MEMORY": "4G",
            "SPARK_EXECUTOR_MEMORY": "4G",
        }, # injeta credenciais no container
        # código local - dentro do container
        mounts = [
            Mount(
                target = "/opt/spark/app/processing",
                source = f"{PROJ_DIR}/processing",
                type = "bind"
            ),
            Mount(
                target = "/opt/spark/app/config",
                source = f"{PROJ_DIR}/config",
                type = "bind"
            ),
            Mount(
                target = "/opt/spark/app/utils",
                source = f"{PROJ_DIR}/utils",
                type = "bind"
            ),
            Mount(
                target = "/opt/spark/conf/hive-site.xml",
                source = f"{PROJ_DIR}/storage/hive/hive-site.xml",
                type = "bind"
            ),
            Mount(
                target = "/opt/spark/app/.env",
                source = f"{PROJ_DIR}/.env",
                type = "bind"
            ),
        ],
        working_dir = "/opt/spark/app",
        network_mode = DOCKER_NETWORK,
        docker_url = "unix://var/run/docker.sock",
        auto_remove = "success",
        mount_tmp_dir = False,
        retrieve_output = False,
        force_pull = False,
        doc_md = (
            "Executa spark-submit do job movies dentro de um container"
            "Produz silver.movies_cleaned e silver.movie_genres - Delta merge"
            "Exit code 0 = sucesso; qualquer outro valor = FAILED + retry."
        )
    )
    # garantir que a dag sempre finalize
    end = EmptyOperator(
        task_id = "end",
        trigger_rule = TriggerRule.ALL_DONE
    )

    # Pipeline
    start >> check_bronze >> run_spark >> end 
