from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

import traceback
from typing import List, Optional

from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

logger = get_logger("silver-movies")

BRONZE_MOVIES_PATH   = "s3a://bronze/batch/movies"
SILVER_CLEANED_PATH  = "s3a://silver/cleaned/movies"
SILVER_ENRICHED_PATH = "s3a://silver/enriched/movie_genres"
 
TITLE_YEAR_PATTERN = r"^(.*)\s\(\d{4}\)$"
YEAR_PATTERN       = r"\((\d{4})\)"
 
SILVER_DB          = "silver"
CLEANED_TABLE      = f"{SILVER_DB}.movies_cleaned"
GENRES_TABLE       = f"{SILVER_DB}.movie_genres"
 
MIN_VALID_YEAR = 1888
MAX_VALID_YEAR = 2028

# =============================
# READ
# =============================
def read_bronze(spark: SparkSession) -> DataFrame:

    logger.info(f"Lendo tabela movies da Bronze: {BRONZE_MOVIES_PATH}")

    df = spark.read.format("delta").load(BRONZE_MOVIES_PATH)

    count = df.count()

    if count == 0:
        raise ValueError("Tabela movies está vazia")
    
    logger.info(f"Bronze carregada: {count} registros")
    return df

# =============================
# TRANSFORM — CLEANED
# =============================
def build_cleaned(df: DataFrame) -> DataFrame:

    logger.info("Construindo camada CLEANED")

    # Renomear colunas genres para genres_raw
    df_renamed = df.withColumnRenamed("genres", "genres_raw")

    # Extraindo titulo e Ano 
    df_extracted = (
        df_renamed
        .withColumn(
            "release_year",
            F.regexp_extract(F.col("title"), YEAR_PATTERN, 1).cast(IntegerType())
        )
        .withColumn(
            "title",
            F.trim(F.regexp_extract(F.col("title"), TITLE_YEAR_PATTERN, 1))
        )
    )

    # Descartar titulos nulos ou vazio 
    df_valid_title = df_extracted.filter(
        F.col("title").isNotNull() & (F.trim(F.col("title")) != "")
    )

    # Descartar movies com ano inválido (fora do range)
    df_valid_year = df_valid_title.filter(
        F.col("release_year").isNotNull()
        & F.col("release_year").between(MIN_VALID_YEAR, MAX_VALID_YEAR)
    )

    # Preenche genres_raw nulos com "unknown"
    df_filled = df_valid_year.fillna({"genres_raw": "unknown"})

    # Remover movies duplicados, mantendo os mais recentes 
    window = (
        __import__("pyspark.sql.window", fromlist = ["Windows"])
        .Window.partitionBy("movie_id")
        .orderBy(F.col("_ingestion_timestamp").desc())
    ) 
    df_dedup = (
        df_filled
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Tipar as colunas finais
    df_cleaned = (
        df_dedup
        .select(
            F.col("movie_id").cast(IntegerType()).alias("movie_id"),
            F.col("title").cast(StringType()).alias("title"),
            F.col("release_year").cast(IntegerType()).alias("release_year"),
            F.col("genres_raw").cast(StringType()).alias("genres_raw"),
            F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
            F.col("_source").alias("source"),
            F.current_timestamp().alias("processed_timestamp")
        )
    )

    total_in = df.count()
    total_out = df_cleaned.count()
    dropped = total_in - total_out

    logger.info(f"CLEANED — entrada: {total_in} | saída: {total_out} | descartados: {dropped}")

    return df_cleaned


# =============================
# TRANSFORM — GENRES (N:N)
# =============================
def build_genres(df_cleaned: DataFrame) -> DataFrame:

    logger.info("Construindo tabela GENRES (N:N)")

    df_genres = (
        df_cleaned
        .withColumn("genre", F.explode(F.split(F.col("genres_raw"),  r"\|")))
        .withColumn("genre", F.lower(F.trim(F.col("genre"))))
        .filter(
            F.col("genre").isNotNull()
            & (F.col("genre") != "")
            & (F.col("genre") != "(no genres listed)")
        )
        .select(
            F.col("movie_id").cast(IntegerType()).alias("movie_id"),
            F.col("genre").cast(StringType()).alias("genre"),
        )
        .distinct()
    )

    logger.info(f"GENRES: {df_genres.count()} pares (movie_id, genre) gerados")

    return df_genres

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
        location = SILVER_CLEANED_PATH,
        partition_cols = ["release_year"]
    )

    (
        _delta_table(spark, SILVER_CLEANED_PATH)
        .alias("target")
        .merge(
            df.alias("source"),
            "target.movie_id = source.movie_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info("CLEANED escrita com sucesso")

def write_genres(spark: SparkSession, df: DataFrame) -> None:
    
    logger.info(f"Escrevendo GENRES em: {SILVER_ENRICHED_PATH}")

    _ensure_database(spark, SILVER_DB)

    _create_delta_table_if_not_exists(
        spark,
        df = df,
        table = GENRES_TABLE, 
        location = SILVER_ENRICHED_PATH
    )

    (
        _delta_table(spark, SILVER_ENRICHED_PATH)
        .alias("target")
        .merge(
            df.alias("source"),
            "target.movie_id = source.movie_id AND target.genre = source.genre",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("GENRES escrita com sucesso")


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
    
   from delta.tables import DeltaTable

   if not DeltaTable.isDeltaTable(spark, location):

        writer = (
            df.write
            .format("delta")
            .mode("overwrite")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(location)

def _delta_table(spark: SparkSession, path: str):
    """Retorna um DeltaTable a partir do path"""
    from delta.tables import DeltaTable 
    return DeltaTable.forPath(spark, path)


# =============================
# ORCHESTRATION
# =============================
def process_movies_to_silver(spark: SparkSession) -> None:
    
    logger.info("--- Iniciando pipeline movies Bronze -> Silver ---")

    df_bronze = read_bronze(spark)
    df_cleaned = build_cleaned(df_bronze)
    df_genres = build_genres(df_cleaned)

    df_cleaned_final = df_cleaned.drop("genres_raw")

    write_cleaned(spark, df_cleaned_final)
    write_genres(spark, df_genres)

    logger.info("--- Pipeline finalizado com sucesso ---")

# =============================
# MAIN
# =============================
def main() -> None:
    spark = create_spark_session("silver-movies")

    try:
        process_movies_to_silver(spark)

    except Exception:
        logger.exception("Pipeline falhou com erro não tratado")

        traceback.print_exc()

        raise 

    finally:
        spark.stop()
    

if __name__ == "__main__":
    main()
