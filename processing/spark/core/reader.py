from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from pyspark.sql import functions as F

from processing.spark.core.base_config import BasePipelineConfig


def read_incremental(
    spark: SparkSession, 
    config: BasePipelineConfig,
    from_version: Optional[int]
) -> DataFrame:

    path = config.bronze_path

    if from_version is None:
        return spark.read.format("delta").load(path) 

    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version + 1)
        .load(path)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )   