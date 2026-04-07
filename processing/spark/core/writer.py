from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import Optional, List
from pyspark.sql import functions as F

from processing.spark.core.base_config import BasePipelineConfig


def ensure_database(spark: SparkSession, db: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def _table_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def _create_table(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    path: str,
    partition_cols: Optional[List[str]] = None
):
    if partition_cols and "event_date" in partition_cols:
        df = df.repartition("event_date")

    writer = df.write.format("delta").mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols) 

    writer.save(path)
   
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING DELTA
        LOCATION '{path}'          
    """)


def _build_merge_condition(keys: List[str]) -> str:
    return " AND ".join([f"target.{k} = source.{k}" for k in keys])


def write_cleaned(
    spark: SparkSession, 
    df: DataFrame, 
    config: BasePipelineConfig, 
    logger
) -> None:
    ensure_database(spark, config.database)

    if not _table_exists(spark, config.silver_cleaned_path):
        logger.info(
            f"Tabela cleaned não existe, criando - table: {config.table_cleaned}", 
            extra = {"table": config.table_cleaned}
        )

        _create_table(
            spark, 
            df, 
            config.table_cleaned, 
            config.silver_cleaned_path
        )
        return 
    
    condition = _build_merge_condition(config.merge_keys_cleaned)
    
    (
        DeltaTable.forPath(spark, config.silver_cleaned_path)
        .alias("target")
        .merge(df.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()   
    )

    logger.info(
        f"Cleaned escrito com sucesso - table: {config.table_cleaned}", 
        extra = {"table": config.table_cleaned}
    )


def write_features(
    spark: SparkSession, 
    df: DataFrame, 
    config: BasePipelineConfig, 
    logger
) -> None:

    ensure_database(spark, config.database)

    if not _table_exists(spark, config.silver_enriched_path):
        logger.info(
            f"Tabela features não existe, criando - table: {config.table_enriched}", 
            extra = {"table": config.table_enriched}
        )

        _create_table(
            spark, 
            df, 
            config.table_enriched, 
            config.silver_enriched_path,
            partition_cols = config.partition_cols_features
        )
        return 
    
    if config.merge_keys_features:
        condition = _build_merge_condition(config.merge_keys_features)

        (
            DeltaTable.forPath(spark, config.silver_enriched_path)
            .alias("target")
            .merge(df.alias("source"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        df.write.mode("append").format("delta").save(config.silver_enriched_path)

    logger.info(
        f"Features escritas com sucesso - table: {config.table_enriched}", 
        extra = {"table": config.table_enriched}
    )