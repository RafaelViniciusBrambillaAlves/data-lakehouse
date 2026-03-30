from pyspark.sql import SparkSession, DataFrame
from ingestion.batch.postgres.connection import (
    get_jdbc_url,
    get_connection_properties
)
from utils.logger import get_logger

logger = get_logger("postgres-extract")

def extract_table(spark: SparkSession, table: str) -> DataFrame:
    logger.info(f"Extraindo tabela: {table}")

    df = spark.read.jdbc(
        url = get_jdbc_url(),
        table = f"(SELECT * FROM {table}) AS t",
        properties = get_connection_properties()
    )

    logger.info(f"Tabela {table} extraída com sucesso.")

    if df.limit(1).count() == 0:
        logger.warning(f"Tabela {table} retornou vazia.")
    else:
        logger.info(f"Tabela {table} extraída com sucesso.")

    return df 