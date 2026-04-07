from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from typing import Optional

from processing.spark.core.base_config import BasePipelineConfig

CHECKPOINT_PROPERTY = "silver.last_bronze_version"

def get_last_processed_version(
    spark: SparkSession, 
    config: BasePipelineConfig
) -> Optional[int]:

    try: 
        result = spark.sql(
            f"SHOW TBLPROPERTIES {config.table_cleaned} ('{CHECKPOINT_PROPERTY}')"
        ).collect()

        if result:
            return int(result[0]["value"])
        
        return None
    
    except Exception:
        return None
    
def save_checkpoint(
    spark: SparkSession, 
    config: BasePipelineConfig, 
    version: int
) -> None:

    spark.sql(f"""
        ALTER TABLE {config.table_cleaned}
        SET TBLPROPERTIES ('{CHECKPOINT_PROPERTY}' = '{version}')
    """)


def get_current_bronze_version(
    spark: SparkSession, 
    config: BasePipelineConfig
) -> int:
    
    return (
        DeltaTable.forPath(spark, config.bronze_path)
        .history(1)
        .select("version")
        .collect()[0]["version"]
    )
