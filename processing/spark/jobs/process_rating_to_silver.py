from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window

import traceback
from typing import Optional, List

from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

logger = get_logger("silver-ratings")

BRONZE_RATINGS_PATH = "s3a://bronze/streaming/ratings_for_additional_users"
SILVER_CLEANED_PATH = "s3a://silver/cleaned/ratings"
SILVER_ENRICHED_PATH = "s3a://silver/enriched/rating_features"

SILVER_DB = "silver"
CLEANED_TABLE = f"{SILVER_DB}.ratings_cleaned"
ENRICHED_TABLE = f"{SILVER_DB}.rating_features"

# Escala valida de rating 
MIN_RATING = 0.5
MAX_RATING = 5.0

# Limiar para avaliacao positiva 
POSITIVE_RATING_THRESHOLD = 4.0

# Mapeando de bucket de rating
RATING_BUCKETS = [
    (1.5, "very_low"),
    (2.5, "low"),
    (3.5, "mid"),
    (4.5, "high"),
    (5.0, "top")
] 

# Mapeamento de categoria de rating 
RATING_CATEGORIES = [
    (2.5, "low"), # < 2.5
    (3.5, "medium"), # 2.5 - 3.5
    (5.0, "hith") # 3.5 - 5.0
]


# =============================
# READ
# =============================
def read_bronze(spark: SparkSession) -> DataFrame:
    
    logger.info("Lendo tabela ratings da Bronze: %s", BRONZE_RATINGS_PATH)

    df = spark.read.format("delta").load(BRONZE_RATINGS_PATH)

    count = df.count()

    if count == 0:
        raise ValueError("Tabela ratings está vazia")
    
    logger.info("Bronze carregada: %d registros", count)

    return df


# =============================
# TRANSFORM — CLEANED
# =============================
def build_cleaned(df: DataFrame) -> DataFrame:
    
    logger.info("Construindo camada CLEANED")

    # Renomear Colunas 
    df_renamed = (
        df
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("movieId", "movie_id")
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("_source", "source_system")
        .withColumnRenamed("_topic", "source_topic")
    )

    # Cast de IDs e timestamp 
    df_cast = (
        df_renamed
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
        .withColumn("event_timestamp", F.col("event_timestamp").cast(TimestampType()))
    )

    # Tratamento Rating invalido, 'NA' e fora [0.5 - 5.0] viram NULL 
    df_ratings = df_cast.withColumn(
        "rating",
        F.when(
            F.col("rating").cast(DoubleType()).isNull(),
            F.lit(None).cast(DoubleType())
        ).when(
            F.col("rating").cast(DoubleType()).between(MIN_RATING, MAX_RATING),
            F.col("rating").cast(DoubleType())
        ).otherwise(
            F.lit(None).cast(DoubleType())
        )
    )

    # Descarta chaves obrigatorias nulas
    df_valid = df_ratings.filter(
        F.col("user_id").isNotNull()
        & F.col("movie_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
    )

    # Deduplicação (user_id, movie_id, event_timestamp)
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
        F.col("rating").cast(DoubleType()).alias("rating"),
        F.col("event_timestamp").cast(TimestampType()).alias("event_timestamp"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp"),
        F.col("source_system").cast(StringType()).alias("source_system"),
        F.col("source_topic").cast(StringType()).alias("source_topic"),
        F.current_timestamp().alias("processed_timestamp")
    )

    total_in = df.count()
    total_out = df_cleaned.count()
    dropped = total_in - total_out

    logger.info("CLEANED — entrada: %d | saída: %d | descartados: %d", total_in, total_out, dropped)
    # logger.info("--- CLEANED ---")

    return df_cleaned


# =============================
# TRANSFORM — ENRICHED (FEATURES)
# =============================
def build_features(df_cleaned: DataFrame) -> DataFrame:
    
    logger.info("Construindo camada ENRICHED (rating_features)")

    # rating category 
    rating_category_expr = (
        F.when(F.col("rating").isNull(), F.lit(None).cast(StringType()))
         .when(F.col("rating") <= 2.5, F.lit("low"))
         .when(F.col("rating") <= 3.5, F.lit("medium"))
         .otherwise(F.lit("high"))
    )

    # rating_bucket
    rating_bucket_expr = F.when(F.col("rating").isNull(), F.lit(None).cast(StringType()))

    prev_upper = 0.0
    
    for upper, label in RATING_BUCKETS:
        rating_bucket_expr = rating_bucket_expr.when(
            F.col("rating").between(prev_upper + 0.01 if prev_upper > 0 else MIN_RATING, upper),
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
            "rating_category",
            rating_category_expr
        )
        .withColumn(
            "is_positive",
            F.when(
                F.col("rating").isNotNull(),
                F.col("rating") >= POSITIVE_RATING_THRESHOLD
            ).otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "rating_bucket",
            rating_bucket_expr
        )
        .withColumn(
            "recency_days",
            F.when(
                F.col("event_date").isNotNull(),
                F.datediff(F.current_date(), F.col("event_date"))
            ).otherwise(F.lit(None)).cast(IntegerType())
        )
        .select(
            F.col("user_id"),
            F.col("movie_id"),
            F.col("rating"),
            F.col("rating_category"),
            F.col("is_positive"),
            F.col("rating_bucket"),
            F.col("event_timestamp"),
            F.col("event_date"),
            F.col("recency_days")
        )
    )

    logger.info("ENRICHED — %d registros gerados", df_features.count())
    # logger.info("--- ENRICHED ---")

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
        location = SILVER_CLEANED_PATH,
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
    
    logger.info("Escrevendo ENRICHED em: %s", SILVER_ENRICHED_PATH)

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
def process_ratings_to_silver(spark: SparkSession) -> None:
    
    logger.info("--- Iniciando pipeline ratings Bronze -> Silver ---")

    df_bronze = read_bronze(spark)
    df_cleaned = build_cleaned(df_bronze)
    # df_features = build_features(df_cleaned)

    write_cleaned(spark, df_cleaned)
    # write_features(spark, df_features)

    logger.info("--- Pipeline finalizado com sucesso ---")

# =============================
# MAIN
# =============================
def main() -> None:
    spark = create_spark_session("silver-ratings")

    try: 
        process_ratings_to_silver(spark)

    except Exception:
        logger.exception("Pipeline falhou com erro não tratado")

        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()