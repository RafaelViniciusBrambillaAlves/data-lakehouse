from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType
from delta.tables import DeltaTable

import time
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

# Checkpoint de versão Delta 
CHECKPOINT_PROPERTY = "silver.last_bronze_version"

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
# CHECKPOINT 
# =============================
def _get_last_processed_version(spark: SparkSession) -> Optional[int]:
    """
    Le a ultima versao Bronze processada, retorna None na primeira carga 
    """
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {CLEANED_TABLE}").collect()

        for row in props:
            
            if row["key"] == CHECKPOINT_PROPERTY:
                
                version = int(row["value"])

                logger.info("Checkpoint encontrado: última versão Bronze processada: %d", version)
                return version
            
        logger.info("Propriedade '%s' não encontrada", CHECKPOINT_PROPERTY)
        return None
    
    except Exception:

        logger.info("Tabela %s ainda não existe", CLEANED_TABLE)
        return None
    

def _save_checkpoint(spark: SparkSession, bronze_version: int) -> None:
    """
    Persiste a verssao Bronze processada como propriedade da tabela Silver
    """
    spark.sql(f"""
        ALTER TABLE {CLEANED_TABLE}
        SET TBLPROPERTIES ('{CHECKPOINT_PROPERTY}' = '{bronze_version}')
    """)

    logger.info("Checkpoint salvo: versão Bronze: %d", bronze_version)


def _get_bronze_version(spark: SparkSession) -> int:
    """Retorna a versão atual (mais recente) do Delta log da Bronze"""
    
    return (
        DeltaTable.forPath(spark, BRONZE_HISTORY_PATH)
        .history(1)
        .select("version")
        .collect()[0]["version"]
    )


# =============================
# READ INCREMENTAL 
# =============================
def read_incremental(spark: SparkSession, from_version: Optional[int]) -> DataFrame:
    
    if from_version is None:
        logger.info("Carga inicial: lendo snapshot completo da Bronze")
        return spark.read.format("delta").load(BRONZE_HISTORY_PATH)
    
    logger.info("Leitura incremental: Bronze versões > %d", from_version)

    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version + 1)
        .load(BRONZE_HISTORY_PATH)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


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

    df_rating = (
        df_ts
        .withColumn("_rating_dbl", F.col("predicted_rating").cast(DoubleType()))
        .withColumn(
            "predicted_rating",
            F.when(
                F.col("_rating_dbl").isNull(),
                F.lit(None).cast(DoubleType())
            ).when(
                F.col("_rating_dbl").between(MIN_PREDICTED_RATING, MAX_PREDICTED_RATING),
                F.col("_rating_dbl")
            ).otherwise(
                F.lit(None).cast(DoubleType())
            )
        )
        .drop("_rating_dbl")    
    )

    # Descartar chaves obrigatorias nulas
    df_valid = df_rating.filter(
        F.col("user_id").isNotNull()
        & F.col("movie_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
    )

    # Deduplicacao por (user_id, movie_id, event_timestap)
    df_dedup = df_valid.dropDuplicates(["user_id", "movie_id", "event_timestamp"])

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
        df.write
        .format("delta")
        .mode("append")
        .save(SILVER_CLEANED_PATH)
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

    date_range = (
        df
        .select(
            F.min("event_date").alias("min_date"),
            F.max("event_date").alias("max_date")
        )
        .collect()[0]
    )

    if date_range["min_date"] is None:
        
        logger.warning("Lote sem event_date válido: escrita ENRICHED ignorada")
        return 

    min_date = date_range["min_date"]
    max_date = date_range["max_date"]
    
    replace_condition = f"event_date >= '{min_date}' AND event_date <= '{max_date}'"

    logger.info("replaceWhere: %s", replace_condition)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .partitionBy("event_date")
        .save(SILVER_ENRICHED_PATH)
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

        logger.info("Tabela Delta criada: %s", table)


def _delta_table(spark: SparkSession, path: str):
    return DeltaTable.forPath(spark, path )


# =============================
# ORCHESTRATION
# =============================
def process_history_to_silver(spark: SparkSession) -> None:

    start_time = time.time()
    
    logger.info("--- Iniciando pipeline user_recommendation_history Bronze -> Silver ---")

    # Checkpoint: versão Bronze já processada
    last_version = _get_last_processed_version(spark)
    current_version = _get_bronze_version(spark) 

    if last_version is not None and last_version >= current_version:
        logger.info(
            "Bronze na versão %d já processada: nenhum dado novo. Pipeline encerrado", current_version
        )
        return

    logger.info("Processando Bronze versões %s -> %d",
        f"{last_version + 1}" if last_version is not None else "inicial",
        current_version,
    )

    df_incremental  = read_incremental(spark, last_version)

    if df_incremental.isEmpty():
        logger.info("Lote incremental vazio após - Pipeline encerrado")
        return 
    
    df_cleaned = build_cleaned(df_incremental)
    df_features = build_features(df_cleaned)

    write_cleaned(spark, df_cleaned)
    write_features(spark, df_features)

    _save_checkpoint(spark, current_version)

    end_time = time.time()
    duration = end_time - start_time

    logger.info(f"--- Pipeline finalizado com sucesso em {duration:.2f} segundos ---")


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