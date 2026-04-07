from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

from processing.spark.jobs.recommendation_history.pipeline import RecommendationHistoryPipeline
from processing.spark.jobs.recommendation_history.recommendation_history_config import CONFIG

def main():

    spark = create_spark_session("silver-recommendation-history")
    logger = get_logger("recommendation-history-pipeline")

    try: 
        pipeline = RecommendationHistoryPipeline(
            spark = spark,
            config = CONFIG,
            logger = logger
        )
        
        pipeline.run()

    except Exception:
        logger.exception("Erro ao executar pipeline recommendation_history")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
