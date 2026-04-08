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
DAG_ID = "silver_belief"

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_JOB = "/opt/spark/app/processing/spark/jobs/belief/main.py"

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
    "retry_delay": timedelta(minutes = 5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes = 20),
    "execution_timeout": timedelta(hours = 2),
    "sla": timedelta(minutes = 30)
}


# CALLBACKS
def _on_failure(context: dict) -> None:
    ti = context["task_instance"] 
    log.error(
        "Falha | dag=%s | task=%s | run_id=%s | try=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
    )

def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    log.warning(
        "SLA miss | dag=%s | tasks bloqueadas=%s",
        dag.dag_id,
        [t.task_id for t in blocking_tis]
    )


# DAG
with DAG(
    dag_id = DAG_ID,
    description = "Pipeline Bronze -> Silver: belief_cleaned e belief_features",
    schedule_interval = "0 3 * * *",
    start_date = datetime(2026, 1, 1),
    catchup = False,
    max_active_runs = 1,
    default_args = default_args,
    on_failure_callback = _on_failure,
    sla_miss_callback = _on_sla_miss,
    tags = ["silver", "belief", "spark", "batch"]
) as dag:
    
    start = EmptyOperator(task_id = "start")

    check_bronze = S3KeySensor(
        task_id = "check_bronze",
        bucket_name = "bronze",
        bucket_key = "batch/belief_data/*",
        wildcard_match = True,
        aws_conn_id = AWS_CONN_ID,
        poke_interval = 60, 
        timeout = 1800,
        mode = "reschedule",
        soft_fail = True
    )

    run_spark = DockerOperator(
        task_id = "run_spark_pipeline",
        image = SPARK_IMAGE,
        container_name = f"airflow__{DAG_ID}__{{{{ ts_nodash }}}}",
        command = (
            f"{SPARK_SUBMIT} "
            "--conf spark.jars.ivy=/tmp/.ivy "
            f"{SPARK_JOB}"
        ),
        environment = {
             "AWS_ACCESS_KEY_ID": settings.AWS_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": settings.AWS_SECRET_KEY,
            "S3_ENDPOINT": settings.S3_ENDPOINT,
            "PYTHONPATH": "/opt/spark/app",
            "SPARK_DRIVER_MEMORY": "4G",
            "SPARK_EXECUTOR_MEMORY": "4G",
        },
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
            "Executa spark-submit do job belief dentro de um container"
            "Produz silver.belief_cleaned e silver.belief_features - Delta merge"
            "Exit code 0 = sucesso; qualquer outro valor = FAILED + retry."
        )
    )

    end = EmptyOperator(
        task_id = "end",
        trigger_rule = TriggerRule.ALL_DONE
    )


    # Pipeline
    start >> check_bronze >> run_spark >> end 
