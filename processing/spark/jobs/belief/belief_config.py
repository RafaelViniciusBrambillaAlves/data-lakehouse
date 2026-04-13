from dataclasses import dataclass
from processing.spark.core.base_config import BasePipelineConfig


@dataclass(frozen = True)
class BeliefConfig(BasePipelineConfig):
    pass

CONFIG = BeliefConfig(
    name = "belief",
    database = "silver",
    bronze_path = "s3a://lakehouse/bronze.db/batch/belief_data",

    silver_cleaned_path = "s3a://lakehouse/silver.db/cleaned/belief",
    silver_enriched_path = "s3a://lakehouse/silver.db/enriched/belief_features",

    table_cleaned = "silver.belief_cleaned",
    table_enriched = "silver.belief_features",

    merge_keys_cleaned = ["user_id", "movie_id", "event_timestamp"],
    merge_keys_features = ["user_id", "movie_id", "event_timestamp"],

    partition_cols_cleaned = None,
    partition_cols_features = ["event_date"]
)
