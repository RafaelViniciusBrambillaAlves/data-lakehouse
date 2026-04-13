from airflow import DAG
from plugins.dag_factory import make_silver_dag

silver_movies = make_silver_dag(
    dag_id = "silver_movies",
    spark_job = "jobs/movies/main.py",
    bronze_key = "bronze.db/batch/movies/*",
    description = "Pipeline Bronze -> Silver: movies_cleaned e movie_genres",
    tags = ["silver", "movies", "batch", "spark"]
)