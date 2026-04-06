from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from delta.tables import DeltaTable
import time

import traceback
from typing import List, Optional

from processing.spark.utils.spark_session import create_spark_session
from utils.logger import get_logger

logger = get_logger("silver-movies")

BRONZE_MOVIES_PATH = "s3a://bronze/batch/movies"
SILVER_CLEANED_PATH = "s3a://silver/cleaned/movies"
SILVER_ENRICHED_PATH = "s3a://silver/enriched/movie_genres"

SILVER_DB = "silver"
CLEANED_TABLE = f"{SILVER_DB}.movies_cleaned"
GENRES_TABLE = f"{SILVER_DB}.movie_genres"

# Checkpoint 
CHECKPOINT_PROPERTY = "silver.last_bronze_version"

# Regex Titulo e Ano
TITLE_YEAR_PATTERN = r"^(.*)\s\(\d{4}\)$"
YEAR_PATTERN = r"\((\d{4})\)"

# Ano Maximo e Minimo
MIN_VALID_YEAR = 1888
MAX_VALID_YEAR = 2028


# =============================
# CHECKPOINT 
# =============================
def _get_last_processed_version(spark: SparkSession) -> Optional[int]:
    
    try:
        props = spark.sql("SHOW TBLPROPERTIES {CLEANED_TABLE}").collect()

        for row in props:
            
            if row["key"] == CHECKPOINT_PROPERTY:

                version = int(row["value"])

                logger.info("Checkpoint encontrado — última versão Bronze processada: %d", version)
                return version 
        
        logger.info("Propriedade '%s' não encontrada — carga inicial.", CHECKPOINT_PROPERTY)
        return None
    
    except Exception:

        logger.info("Tabela %s ainda não existe — carga inicial.", CLEANED_TABLE)
        return None 
    

def _save_checkpoint(spark: SparkSession, bronze_version: int) -> None:

    spark.sql(f"""
        ALTER TABLE {CLEANED_TABLE}
        SET TBLPROPERTIES ('{CHECKPOINT_PROPERTY}' == '{bronze_version}')
    """)
    logger.info("Checkpoint salvo — versão Bronze: %d", bronze_version)


def _get_bronze_version(spark: SparkSession) -> int:

    return (
        DeltaTable.forPath(spark, BRONZE_MOVIES_PATH)
        .history(1)
        .select("version")
        .collect()[0]["version"]
    )


# =============================
# READ INCREMENTAL
# =============================
def read_incremental(spark: SparkSession, from_version: int) -> DataFrame:

    if from_version is None:

        logger.info("Carga inicial: lendo snapshot completo da Bronze.")
        return spark.read.format("delta").load(BRONZE_MOVIES_PATH)

    logger.info("Leitura incremental: Bronze versões > %d", from_version)

    return (
        spark.read
        .format("delta")
        .option("readChangeVersion", "true")
        .option("startingVersion", from_version + 1)
        .load(BRONZE_MOVIES_PATH)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


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
    df_dedup = df_filled.dropDuplicates(["movie_id"])

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

    return df_genres


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
        partition_cols = ["release_year"]
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .save(SILVER_CLEANED_PATH)
    )

    logger.info("CLEANED escrita com sucesso")


def write_genres(spark: SparkSession, df: DataFrame) -> None:
    
    logger.info("Escrevendo GENRES em: %s", SILVER_ENRICHED_PATH)

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
def process_movies_to_silver(spark: SparkSession) -> None:

    start_time = time.time()
    
    logger.info("--- Iniciando pipeline movies Bronze -> Silver ---")

    last_version = _get_last_processed_version(spark)
    current_version = _get_bronze_version(spark)

    if last_version is not None and last_version >= current_version:
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
        logger.info("Lote incremental vazio. Pipeline encerrado")

    df_cleaned = build_cleaned(df_incremental)
    df_genres = build_genres(df_cleaned)

    write_cleaned(spark, df_cleaned.drop("genres_raw"))
    write_genres(spark, df_genres)

    _save_checkpoint(spark, current_version)

    end_time = time.time()
    duration = end_time - start_time

    logger.info(f"--- Pipeline finalizado com sucesso em {duration:.2f} segundos ---")


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