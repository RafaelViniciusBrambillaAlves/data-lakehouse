from airflow import DAG 
from plugins.dag_factory import make_silver_dag

silver_movie_elicitation = make_silver_dag(
    dag_id = "silver_movie_elicitation",
    spark_job = "jobs/movie_elicitation/main.py",
    bronze_key = "batch/movie_elicitation_set/*",
    description = "Pipeline Bronze -> Silver: movie_elicitation_set e elicitation_features",
    tags = ["silver", "elicitation", "batch", "spark"]
)