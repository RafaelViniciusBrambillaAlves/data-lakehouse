from airflow import DAG
from plugins.dag_factory import make_silver_dag

silver_ratings = make_silver_dag(
    dag_id = "silver_ratings",
    spark_job = "jobs/ratings/main.py",
    bronze_key = "bronze.db/streaming/ratings_for_additional_users/*",
    description = "Pipeline Bronze -> Silver: ratings",
    tags = ["silver", "ratings", "batch", "spark"],
)
