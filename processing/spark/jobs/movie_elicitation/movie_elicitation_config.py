from dataclasses import dataclass
from processing.spark.core.base_config import BasePipelineConfig

@dataclass(frozen = True)
class MovieElicitationConfig(BasePipelineConfig):
    pass

CONFIG = MovieElicitationConfig(
    name = "elicitation",
    database = "silver",
    bronze_path = "s3a://lakehouse/bronze.db/batch/movie_elicitation_set",

    silver_cleaned_path = "s3a://lakehouse/silver.db/cleaned/movie_elicitation_set",
    silver_enriched_path = "s3a://lakehouse/silver.db/enriched/elicitation_features",

    table_cleaned = "silver.elicitation_cleaned",
    table_enriched = "silver.elicitation_features",

    merge_keys_cleaned = ["movie_id", "event_timestamp"],
    merge_keys_features = ["movie_id", "event_timestamp"],

    partition_cols_features = ["event_date"]
)

