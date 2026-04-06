from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType
from delta.tables import DeltaTable

import time
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

# Checkpoint 
CHECKPOINT_PROPERTY = "silver.last_bronze_version"

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
# CHECKPOINT 
# =============================
def _get_last_processed_version(spark: SparkSession) -> Optional[int]:
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {CLEANED_TABLE}").collect()

        for row in props:
            
            if row["key"] == CHECKPOINT_PROPERTY:
                
                version = int(row["value"])

                logger.info("Checkpoint encontrado: última versão Bronze processada: %d", version)
                return version
            
        logger.info("Propriedade '%s' não encontrada: carga inicial.", CHECKPOINT_PROPERTY)
        return None
    
    except Exception:

        logger.info("Tabela %s ainda não existe: carga inicial.", CLEANED_TABLE)
        return None
    

def _save_checkpoint(spark: SparkSession, bronze_version: int) -> None:
    spark.sql(f"""
        ALTER TABLE {CLEANED_TABLE}
        SET TBLPROPERTIES ('{CHECKPOINT_PROPERTY}' = '{bronze_version}')
    """)

    logger.info("Checkpoint salvo: versão Bronze: %d", bronze_version)

def _get_bronze_version(spark: SparkSession) -> int:

    return (
        DeltaTable.forPath(spark, BRONZE_RATINGS_PATH)
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
        return spark.read.format("delta").load(BRONZE_RATINGS_PATH)
    
    logger.info("Leitura incremental: Bronze versões > %d", from_version)
    
    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version + 1)
        .load(BRONZE_RATINGS_PATH)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


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
    df_rating = (
        df_cast 
        .withColumn("_rating_dbl", F.col("rating").cast(DoubleType()))
        .withColumn(
            "rating", 
            F.when(
                F.col("_rating_dbl").isNull(),
                F.lit(None).cast(DoubleType())
            ).when(
                F.col("_rating_dbl").between(MIN_RATING, MAX_RATING),
                F.col("_rating_dbl")
            ).otherwise(
                F.lit(None).cast(DoubleType())
            )
        )
        .drop("_rating_dbl")
    )

    # Descarta chaves obrigatorias nulas
    df_valid = df_rating.filter(
        F.col("user_id").isNotNull()
        & F.col("movie_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
    )

    # Deduplicação (user_id, movie_id, event_timestamp)
    df_dedup = df_valid.dropDuplicates(["user_id", "movie_id", "event_timestamp"])

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

    prev_upper = MIN_RATING
    
    for upper, label in RATING_BUCKETS:
        rating_bucket_expr = rating_bucket_expr.when(
            F.col("rating").between(prev_upper, upper),
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

    start_time = time.time()    
    logger.info("--- Iniciando pipeline ratings Bronze -> Silver ---")

    last_version = _get_last_processed_version(spark)
    current_version = _get_bronze_version(spark)

    if last_version is  not None and last_version >= current_version:
        logger.info(
            "Bronze na versão %d já processada: nenhum dado novo. Pipeline encerrado.",
            current_version,
        )
        return 
    
    logger.info(
        "Processando Bronze versões %s -> %d",
        f"{last_version + 1}" if last_version is not None else "inicial",
        current_version,
    )

    df_incremental = read_incremental(spark, last_version)

    if df_incremental.isEmpty():
        logger.info("Lote incremental vazio - Pipeline encerrado")
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