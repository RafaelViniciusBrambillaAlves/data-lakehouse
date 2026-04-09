from airflow import DAG
from plugins.dag_factory import make_silver_dag

silver_recommendation_history = make_silver_dag(
    dag_id = "silver_recommendation_history",
    spark_job = "jobs/recommendation_history/main.py",
    bronze_key = "streaming/user_recommendation_history/*",
    description = "Pipeline Bronze -> Silver: history_cleaned e recommendation_features",
    tags = ["silver", "recommendation_history", "batch", "spark"]
)