from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

from processing.spark.jobs.ratings.pipeline import RatingPipeline
from processing.spark.jobs.ratings.ratings_config import CONFIG

def main():

    spark = create_spark_session("silver-rating")
    logger = get_logger("rating-pipeline")

    try: 
        pipeline = RatingPipeline(
            spark = spark,
            config = CONFIG, 
            logger = logger
        )

        pipeline.run()

    except Exception:
        logger.exception("Erro ao executar pipeline rating")
        raise 

    finally:
        spark.stop()

if __name__ == "__main__":
    main()