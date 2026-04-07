from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

from processing.spark.jobs.movie_elicitation.pipeline import MovieElicitationPipeline
from processing.spark.jobs.movie_elicitation.movie_elicitation_config import CONFIG

def main():

    spark = create_spark_session("silver-movie_elicitation")
    logger = get_logger("movie_elicitation-pipeline")

    try:
        pipeline = MovieElicitationPipeline(
            spark = spark,
            config = CONFIG,
            logger = logger
        )

        pipeline.run()

    except Exception:
        logger.exception("Erro ao executar pipeline movies_elicitation  ")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()