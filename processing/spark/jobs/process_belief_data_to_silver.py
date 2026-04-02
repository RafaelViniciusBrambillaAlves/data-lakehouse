from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window

import traceback

from typing import List, Optional

from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

logger = get_logger("silver-belief")

BRONZE_BELIEF_PATH = "s3a://bronze/batch/belief_data"
SILVER_CLEANED_PATH = "s3a://silver/cleaned/belief"
SILVER_ENRICHED_PATH = "s3a://silver/enriched/belief_features"

SILVER_DB = "silver"
CLEANED_TABLE  = f"{SILVER_DB}.belief_cleaned"
ENRICHED_TABLE = f"{SILVER_DB}.belief_features"

# Usada para não respondeu
SENTINEL_VALUE = -1.0

# Limiar para considerar alta confiança
HIGH_CONFIDENCE_THRESHOLD = 4.0


# =============================
# READ
# =============================
def read_bronze(spark: SparkSession) -> DataFrame:

    logger.info("Lendo tabela belief_data da Bronze: %s", BRONZE_BELIEF_PATH)

    df = spark.read.format("delta").load(BRONZE_BELIEF_PATH)

    count = df.count()

    if count == 0:
        raise ValueError("Tabela belief_data está vazia")
    
    logger.info("Bronze carregada: %d registros", count)

    return df


# =============================
# TRANSFORM — CLEANED
# =============================
def build_cleaned(df: DataFrame) -> DataFrame:

    logger.info("Construindo camada CLEANED")

    # Renomeando colunas 
    df_renamed = (
        df
        .withColumnRenamed("user_elicit_rating", "user_rating")
        .withColumnRenamed("user_predict_rating", "predicted_rating")
        .withColumnRenamed("system_predict_rating", "system_rating")
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("_source", "source_system")
    )

    # Se Sentina -> Null
    df_sentinel = (
        df_renamed
        .withColumn(
            "is_seen",
            F.when(F.col("is_seen") == -1, F.lit(None).cast(IntegerType()))
            .otherwise(F.col("is_seen"))
        )
        .withColumn(
            "user_rating",
            F.when(F.col("user_rating") == SENTINEL_VALUE, F.lit(None).cast(DoubleType()))
            .otherwise(F.col("user_rating"))
        )
        .withColumn(
            "predicted_rating",
            F.when(F.col("predicted_rating") == SENTINEL_VALUE, F.lit(None).cast(DoubleType()))
            .otherwise(F.col("predicted_rating"))
        )
        .withColumn(
            "user_certainty",
            F.when(F.col("user_certainty") == SENTINEL_VALUE, F.lit(None).cast(DoubleType()))
            .otherwise(F.col("user_certainty"))
        )
    ) 

    # Se watch_date = Vazia -> NULL, cast date
    df_dates = df_sentinel.withColumn(
        "watch_date",
        F.when(
            F.col("watch_date").isNull() | (F.trim(F.col("watch_date")) == ""),
            F.lit(None).cast(DateType())
        ).otherwise(
            F.to_date(F.col("watch_date"))
        )
    )

    # Descartar chaves obrigatorias 
    df_valid = df_dates.filter(
        F.col("user_id").isNotNull() & F.col("movie_id").isNotNull()
    )

    # Deduplicação (user_id, movie_id, event_timestamp
    window = (
        Window
        .partitionBy("user_id", "movie_id", "event_timestamp")
        .orderBy(F.col("ingestion_timestamp").desc())
    )

    df_dedup = (
        df_valid
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Tipagem final

    df_cleaned = df_dedup.select(
        F.col("user_id").cast(IntegerType()).alias("user_id"),
        F.col("movie_id").cast(IntegerType()).alias("movie_id"),
        F.col("is_seen").cast(IntegerType()).alias("is_seen"),
        F.col("watch_date").cast(DateType()).alias("watch_date"),
        F.col("user_rating").cast(DoubleType()).alias("user_rating"),
        F.col("predicted_rating").cast(DoubleType()).alias("predicted_rating"),
        F.col("user_certainty").cast(DoubleType()).alias("user_certainty"),
        F.col("system_rating").cast(DoubleType()).alias("system_rating"),
        F.col("event_timestamp").cast(TimestampType()).alias("event_timestamp"),
        F.col("movie_idx").cast(IntegerType()).alias("movie_idx"),
        F.col("source").cast(IntegerType()).alias("source_type"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp"),
        F.col("source_system").cast(StringType()).alias("source_system"),
        F.current_timestamp().alias("processed_timestamp")        
    )

    total_in = df.count()
    total_out = df_cleaned.count()
    dropped = total_in - total_out

    logger.info("CLEANED — entrada: %d | saída: %d | descartados: %d", total_in, total_out, dropped)

    return df_cleaned


# =============================
# TRANSFORM — ENRICHED (FEATURES)
# =============================
def build_features(df_cleaned: DataFrame) -> DataFrame:
    
    logger.info("Construindo camada ENRICHED (belief_features)")

    df_features = (
        df_cleaned
        .withColumn(
            "rating_diff",
            F.when(
                F.col("user_rating").isNotNull() & F.col("system_rating").isNotNull(),
                F.round(F.col("user_rating") - F.col("system_rating"), 4)
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn(
            "is_high_confidence",
            F.when(
                F.col("user_certainty").isNotNull(),
                F.col("user_certainty") >= HIGH_CONFIDENCE_THRESHOLD
            ).otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "has_watched",
            F.col("is_seen") == 1
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp"))
        )
        .select(
            F.col("user_id"),
            F.col("movie_id"),
            F.col("is_seen"),
            F.col("has_watched"),
            F.col("user_rating"),
            F.col("system_rating"),
            F.col("rating_diff"),
            F.col("user_certainty"),
            F.col("is_high_confidence"),
            F.col("event_timestamp"),
            F.col("event_date")
        )
    )

    logger.info("ENRICHED: %d registros gerados", df_features.count())

    return df_features


# =============================
# WRITE
# =============================
def write_cleaned(spark: SparkSession, df: DataFrame) -> None:
    
    logger.info("Escrevendo CLEANED em: %s", SILVER_CLEANED_PATH)

    _ensure_database(spark, SILVER_DB)

    _create_delta_table_if_not_exists(
        spark, 
        df = df,
        table = CLEANED_TABLE, 
        location = SILVER_CLEANED_PATH
    )

    # (user_id, movie_id, event_timestamp)
    (
        _delta_table(spark, SILVER_CLEANED_PATH)
        .alias("target")
        .merge(
            df.alias("source"),
            """
            target.user_id = source.user_id
            AND target.movie_id = source.movie_id
            AND target.event_timestamp = source.event_timestamp
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info("CLEANED escrita com sucesso")


def write_features(spark: SparkSession, df: DataFrame) -> None:
    
    logger.info("Escrevendo ENRICHED em: %s", SILVER_ENRICHED_PATH)

    _ensure_database(spark, SILVER_DB)

    _create_delta_table_if_not_exists(
        spark,
        df = df,
        table = ENRICHED_TABLE,
        location = SILVER_ENRICHED_PATH,
        partition_cols = ["event_date"]
    )

    # (user_id, movie_id, event_timestamp)
    (
        _delta_table(spark, SILVER_ENRICHED_PATH)
        .alias("target")
        .merge(
            df.alias("source"),
            """
            target.user_id = source.user_id
            AND target.movie_id = source.movie_id
            AND target.event_timestamp = source.event_timestamp
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info("ENRICHED escrita com sucesso")


# =============================
# HELPERS
# =============================
def _ensure_database(spark: SparkSession, db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def _create_delta_table_if_not_exists(
        spark: SparkSession,
        df: DataFrame,
        table: str,
        location: str,
        partition_cols: Optional[List[str]] = None
) -> None:
    
    

    if not DeltaTable.isDeltaTable(spark, location):
        writer = df.write.format("delta").mode("overwrite")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(location)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table}
            USING DELTA
            LOCATION '{location}'
        """)

        logger.info("Tabela Delta criada: %s", table)


def _delta_table(spark: SparkSession, path: str):
    return DeltaTable.forPath(spark, path)


# =============================
# ORCHESTRATION
# =============================
def process_belief_to_silver(spark: SparkSession) -> None:
    
    logger.info("--- Iniciando pipeline belief Bronze -> Silver ---")

    df_bronze = read_bronze(spark)
    df_cleaned = build_cleaned(df_bronze)
    df_features = build_features(df_cleaned)

    write_cleaned(spark, df_cleaned)
    write_features(spark, df_features)

    logger.info("--- Pipeline finalizado com sucesso ---")


# =============================
# MAIN
# =============================
def main():
    spark = create_spark_session("silver-belief")

    try:
        process_belief_to_silver(spark)

    except Exception:
        
        logger.info("Pipeline falhou com erro não tratado")

        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    