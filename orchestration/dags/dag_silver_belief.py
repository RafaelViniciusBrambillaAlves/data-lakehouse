from airflow import DAG
from plugins.dag_factory import make_silver_dag

silver_belief = make_silver_dag(
    dag_id = "silver_belief",
    spark_job = "jobs/belief/main.py",
    bronze_key = "batch/belief_data/*",
    description = "Pipeline Bronze -> Silver: belief_cleaned e belief_features",
    tags = ["silver", "belief", "spark", "batch"]
)