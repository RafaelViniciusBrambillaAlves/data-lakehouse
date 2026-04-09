from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

from processing.spark.jobs.movies.pipeline import MoviesPipeline
from processing.spark.jobs.movies.movies_config import CONFIG


def main():

    spark = create_spark_session("silver-movies")
    logger = get_logger("movies-pipeline")
    
    try:
        pipeline = MoviesPipeline(
            spark = spark,
            config = CONFIG,
            logger = logger
        )

        pipeline.run()

    except Exception:
        logger.exception("Erro no pipeline")
        raise

    finally:
        
        spark.stop()


if __name__ == '__main__':
    main()