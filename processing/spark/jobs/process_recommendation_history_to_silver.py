from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window

import traceback
from typing import Optional, List

from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

logger = get_logger("silver-recommendation-history")

BRONZE_HISTORY_PATH = "s3a://bronze/streaming/user_recommendation_history"
SILVER_CLEANED_PATH = "s3a://silver/cleaned/recommendation_history"
SILVER_ENRICHED_PATH = "s3a://silver/enriched/recommendation_features"

SILVER_DB = "silver"
CLEANED_TABLE = f"{SILVER_DB}.recommendation_history_cleaned"
ENRICHED_TABLE = f"{SILVER_DB}.recommendation_features"

# Range valido predicted_rating
MIN_PREDICTED_RATING = 0.0
MAX_PREDICTED_RATING = 5.0

# Limiar para recomendação forte 
HIGH_SCORE_THRESHOULD = 4.0

# Buckets de predicted_rating
PREDICTED_RATING_BUCKETS = [
    (1.5, "very_low"),
    (2.5, "low"),
    (3.5, "mid"),
    (4.5, "high"),
    (5.0, "top")
]


# =============================
# READ
# =============================
def read_bronze(spark: SparkSession) -> DataFrame:
    
    logger.info(f"Lendo tabela user_recommendation_history da Bronze: {BRONZE_HISTORY_PATH}")

    df = spark.read.format("delta").load(BRONZE_HISTORY_PATH)

    count = df.count()

    if count == 0:
        raise ValueError("Tabela user_recommendation_history está vazia")
    
    logger.info(f"Bronze carregada: {count} registros")

    return df


# =============================
# TRANSFORM — CLEANED
# =============================
def build_cleaned(df: DataFrame) -> DataFrame:
    
    logger.info("Construindo camada CLEANED")

    # Renomear colunas
    df_renamed = (
        df
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("movieId", "movie_id")
        .withColumnRenamed("predictedRating", "predicted_rating")
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("_source", "source_system")
        .withColumnRenamed("_topic", "source_topic")
    )

    # Cast IDs
    df_cast = (
        df_renamed
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
    )

    # Cast tstamp 
    df_ts = df_cast.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_timestamp").cast(DoubleType()))
    )

    # predicted_rating: double, fora do range [0.0–5.0] -> NULL
    df_rating = df_ts.withColumn(
        "predicted_rating",
        F.when(
            F.col("predicted_rating").cast(DoubleType()).isNull(),
            F.lit(None).cast(DoubleType())
        ).when(
            F.col("predicted_rating").cast(DoubleType())
             .between(MIN_PREDICTED_RATING, MAX_PREDICTED_RATING),
            F.col("predicted_rating").cast(DoubleType())
        ).otherwise(
            F.lit(None).cast(DoubleType())
        )
    )

    # Descartar chaves obrigatorias nulas
    df_valid = df_rating.filter(
        F.col("user_id").isNotNull()
        & F.col("movie_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
    )

    # Deduplicacao por (user_id, movie_id, event_timestap)
    

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
        F.col("predicted_rating").cast(DoubleType()).alias("predicted_rating"),
        F.col("event_timestamp").cast(TimestampType()).alias("event_timestamp"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp"),
        F.col("source_system").cast(StringType()).alias("source_system"),
        F.col("source_topic").cast(StringType()).alias("source_topic"),
        F.current_timestamp().alias("processed_timestamp")
    )

    total_in = df.count()
    total_out = df_cleaned.count()
    dropped = total_in - total_out

    logger.info(f"CLEANED — entrada: {total_in} | saída: {total_out} | descartados: {dropped}")

    return df_cleaned


# =============================
# TRANSFORM — ENRICHED (FEATURES)
# =============================
def build_features(df_cleaned: DataFrame) -> DataFrame:

    logger.info("Construindo camada ENRICHED (recommendation_features)")

    # predicted_rating_category
    rating_category_expr = (
        F.when(F.col("predicted_rating").isNull(), F.lit(None).cast(StringType()))
         .when(F.col("predicted_rating") <= 2.5, F.lit("low"))
         .when(F.col("predicted_rating") <= 3.5, F.lit("medium"))
         .otherwise(F.lit("high"))
    )    

    # predicted_rating_bucket
    rating_bucket_expr = F.when(F.col("predicted_rating").isNull(), F.lit(None).cast(StringType()))

    prev_upper = MIN_PREDICTED_RATING

    for upper, label in PREDICTED_RATING_BUCKETS:
        rating_bucket_expr = rating_bucket_expr.when(
            F.col("predicted_rating").between(prev_upper, upper),
            F.lit(label)
        )
        prev_upper = upper 

    rating_bucket_expr = rating_bucket_expr.otherwise(F.lit(None).cast(StringType()))

    df_features = (
        df_cleaned
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp"))
        )
        .withColumn(
            "predicted_rating_category",
            rating_category_expr
        )
        .withColumn(
            "is_high_score",
            F.when(
                F.col("predicted_rating").isNotNull(),
                F.col("predicted_rating") >= HIGH_SCORE_THRESHOULD
            ).otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "predicted_rating_bucket",
            rating_bucket_expr
        )
        .select(
            F.col("user_id"),
            F.col("movie_id"),
            F.col("predicted_rating"),
            F.col("predicted_rating_category"),
            F.col("is_high_score"),
            F.col("predicted_rating_bucket"),
            F.col("event_timestamp"),
            F.col("event_date")
        )
    )

    logger.info(f"ENRICHED: {df_features.count()} registros gerados")

    return df_features


# =============================
# WRITE
# =============================
def write_cleaned(spark: SparkSession, df: DataFrame) -> None:
    
    logger.info(f"Escrevendo CLEANED em: {SILVER_CLEANED_PATH}")

    _ensure_database(spark, SILVER_DB)

    _create_delta_table_if_not_exists(
        spark,
        df = df,
        table = CLEANED_TABLE,
        location = SILVER_CLEANED_PATH 
    )

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
     
    logger.info(f"Escrevendo ENRICHED em: {SILVER_ENRICHED_PATH}")

    _ensure_database(spark, SILVER_DB)

    _create_delta_table_if_not_exists(
        spark,
        df = df,
        table = ENRICHED_TABLE,
        location = SILVER_ENRICHED_PATH,
        partition_cols = ["event_date"]
    )

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
    partition_cols:  Optional[List[str]] = None
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

        logger.info("Tabela Delta criada: {table}")


def _delta_table(spark: SparkSession, path: str):
    return DeltaTable.forPath(spark, path )


# =============================
# ORCHESTRATION
# =============================
def process_history_to_silver(spark: SparkSession) -> None:
    
    logger.info("--- Iniciando pipeline user_recommendation_history Bronze -> Silver ---")

    df_bronze = read_bronze(spark)
    df_cleaned = build_cleaned(df_bronze)
    df_features = build_features(df_cleaned)

    write_cleaned(spark, df_cleaned)
    write_features(spark, df_features)

    logger.info("--- Pipeline finalizado com sucesso ---")


# =============================
# MAIN
# =============================
def main() -> None:

    spark = create_spark_session("silver-recommendation-history")

    try: 
        process_history_to_silver(spark)

    except Exception:

        logger.info("Pipeline falhou com erro não tratado")

        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()