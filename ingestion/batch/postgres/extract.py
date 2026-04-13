from pyspark.sql import SparkSession, DataFrame
from ingestion.batch.postgres.connection import (
    get_jdbc_url,
    get_connection_properties
)
from utils.logger import get_logger
from config.settings import settings

logger = get_logger("postgres-extract")

def extract_table(spark: SparkSession, table: str) -> DataFrame:
    logger.info(f"Extraindo tabela: {table}")

    df = (
        spark.read
        .format("jdbc")
        .option("url", get_jdbc_url())
        .option("dbtable", table)
        .option("user", settings.POSTGRES_USER)
        .option("password", settings.POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 1000)
        .load()
    )

    logger.info(f"Tabela {table} extraída com sucesso.")

    if df.limit(1).count() == 0:
        logger.warning(f"Tabela {table} retornou vazia.")
    else:
        logger.info(f"Tabela {table} extraída com sucesso.")

    return df 