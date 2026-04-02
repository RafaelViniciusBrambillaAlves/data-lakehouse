from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window

import traceback

from typing import Optional, List

from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

logger = get_logger("silver-elicitation")

BRONZE_ELICITATION_PATH = "s3a://bronze/batch/movie_elicitation_set"
SILVER_CLEANED_PATH = "s3a://silver/cleaned/movie_elicitation_set"
SILVER_ENRICHED_PATH = "s3a://silver/enriched/elicitation_features"

SILVER_DB = "silver"
CLEANED_TABLE = f"{SILVER_DB}.elicitation_cleaned"
ENRICHED_TABLE = f"{SILVER_DB}.elicitation_features"

# Dominio valido para source_type 
VALID_SOURCE_TYPES = {1, 2, 3, 4, 5}

# Mapeamento categórico - source_type 
SOURCE_CATEGORY_MAP = {
    1: "popular", # popular
    2: "well_rated", # bem avaliado
    3: "recent_popular", # recentemente popular
    4: "trending", # em alta, tendencia
    5: "serendipity" # inesperado
}

# Janela de dias recentes 
RECENCY_DAYS = 90


# =============================
# READ
# =============================
def read_bronze(spark: SparkSession) -> DataFrame:
    
    logger.info("Lendo tabela movie_elicitation_set da Bronze: %s", BRONZE_ELICITATION_PATH)

    df = spark.read.format("delta").load(BRONZE_ELICITATION_PATH)

    count = df.count()
    
    if count == 0:
        raise ValueError("Tabela movie_elicitation_set está vazia")

    logger.info("Bronze carregada: %d registros", count)
    return df


# =============================
# TRANSFORM — CLEANED
# =============================
def build_cleaned(df: DataFrame) -> DataFrame:
    
    logger.info("Construindo camada CLEANED")

    # Renomear colunas
    df_renamed = (
        df
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("source", "source_type")
        .withColumnRenamed("_source", "source_system")
    )

    # Descartar chaves obrigatorias nulas (movie_id, event_timestamp)
    df_valid = df_renamed.filter(
        F.col("movie_id").isNotNull() & F.col("event_timestamp").isNotNull()
    )

    # Valdacao de dominio: source_type fora de [1 - 5] = NULL
    df_domain = df_valid.withColumn(
        "source_type",
        F.when(
            F.col("source_type").isin(*VALID_SOURCE_TYPES),
            F.col("source_type")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # Validacao de dominio: month_idx negativo = NULL
    df_month = df_domain.withColumn(
        "month_idx",
        F.when(
            F.col("month_idx").isNotNull() & (F.col("month_idx") >= 0),
            F.col("month_idx")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # Deduplicacao (movie_id, event_timestamp) - mantem o registro mais recente
    window = (
        Window
        .partitionBy("movie_id", "event_timestamp")
        .orderBy(F.col("ingestion_timestamp").desc())
    )

    df_dedup = (
        df_month
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Tipagem final 
    df_cleaned = df_dedup.select(
        F.col("movie_id").cast(IntegerType()).alias("movie_id"),
        F.col("month_idx").cast(IntegerType()).alias("month_idx"),
        F.col("source_type").cast(IntegerType()).alias("source_type"),
        F.col("event_timestamp").cast(TimestampType()).alias("event_timestamp"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp"),
        F.col("source_system").cast(StringType()).alias("source_system"),
        F.current_timestamp().alias("processd_timestamp")
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
    
    logger.info("Construindo camada ENRICHED (elicitation_features)")

    # Monta expresao CASE para source_category a partir do dicionario de constantes
    source_category_expr = F.when(F.lit(False), F.lit(None))

    for code, label in SOURCE_CATEGORY_MAP.items():
        source_category_expr = source_category_expr.when(
            F.col("source_type") == code, F.lit(label)
        )

    source_category_expr = source_category_expr.otherwise(F.lit(None).cast(StringType()))

    df_features = (
        df_cleaned
        .withColumn(
            "event_date", 
            F.to_date(F.col("event_timestamp"))
        )
        .withColumn(
            "source_category",
            source_category_expr
        )
        .withColumn(
            "month_group",
            F.when(F.col("month_idx").isNull(), F.lit("unknown"))
             .when(F.col("month_idx").between(0, 2), F.lit("early_stage"))
             .when(F.col("month_idx").between(3, 6), F.lit("mid_stage"))
             .otherwise(F.lit("late_stage"))
        )
        .withColumn(
            "is_recent",
            F.col("event_timestamp") >= (F.current_date() - F.expr(f"INTERVAL {RECENCY_DAYS} DAYS"))
        )
        .select(
            F.col("movie_id"),
            F.col("month_idx"),
            F.col("source_type"),
            F.col("source_category"),
            F.col("month_group"),
            F.col("event_timestamp"),
            F.col("event_date"),
            F.col("is_recent"),
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

    (
        _delta_table(spark, SILVER_CLEANED_PATH)
        .alias("target")
        .merge(
            df.alias("source"),
            """
            target.movie_id = source.movie_id
            AND target.event_timestamp = source.event_timestamp
            """,
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
            target.movie_id = source.movie_id
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
def process_elicitation_to_silver(spark: SparkSession) -> None:
    
    logger.info("--- Iniciando pipeline movie_elicitation_set Bronze -> Silver ---")

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
    
    spark = create_spark_session("silver-elicitation")

    try:
        process_elicitation_to_silver(spark)

    except Exception:
        logger.exception("Pipeline falhou com erro não tratado")

        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()