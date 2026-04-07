from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

from processing.spark.jobs.belief.pipeline import BeliefPipeline
from processing.spark.jobs.belief.belief_config import CONFIG


def main():
    spark = create_spark_session("silver-belief")

    logger = get_logger("belief-pipeline")

    try:
        pipeline = BeliefPipeline(
            spark = spark,
            config = CONFIG,
            logger = logger
        )

        pipeline.run()

    except Exception as e:
        logger.exception("Erro ao executar pipeline belief")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()