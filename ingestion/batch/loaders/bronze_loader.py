from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from utils.logger import get_logger

logger = get_logger("bronze-loader")


def load_to_bronze(df: DataFrame, table_name: str, spark: SparkSession) -> str:
    logger.info(f"Salvando tabela {table_name} na Bronze")

    df = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit("postgres"))
    )

    path = f"s3a://bronze/batch/{table_name}"
    
    df = df.repartition(10)

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")  
        .save(path)
    )

    logger.info(f"Tabela {table_name} salva em {path}")

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)

    logger.info(f"Tabela '{table_name}' registrada no Hive Metastore (bronze.{table_name})")

    return path