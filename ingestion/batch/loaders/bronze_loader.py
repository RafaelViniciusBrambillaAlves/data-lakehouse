from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from utils.logger import get_logger

logger = get_logger("bronze-loader")


def load_to_bronze(df: DataFrame, table_name: str, spark: SparkSession) -> str:

    if df is None or df.rdd.isEmpty():
        logger.warning(f"DataFrame da tabela {table_name} está vazio. Ignorando.")
        return None

    path = f"s3a://bronze/batch/{table_name}"

    logger.info(f"Iniciando ingestão da tabela {table_name} na Bronze")

    df = df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source", lit("postgres"))


    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )

    logger.info(f"Tabela {table_name} salva em {path}")

    # Registro no Hive Metastore
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)

    logger.info(f"Tabela '{table_name}' registrada no Hive Metastore (bronze.{table_name})")

    return path