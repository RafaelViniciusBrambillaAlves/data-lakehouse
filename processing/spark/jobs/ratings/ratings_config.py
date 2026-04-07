from dataclasses import dataclass
from processing.spark.core.base_config import BasePipelineConfig

@dataclass(frozen = True)
class RatingsConfig(BasePipelineConfig):
    pass


CONFIG = RatingsConfig(
    name = "ratings",
    database = "silver",
    bronze_path = "s3a://bronze/streaming/ratings_for_additional_users",

    silver_cleaned_path = "s3a://silver/cleaned/ratings",
    silver_enriched_path = "s3a://silver/enriched/rating_features",

    table_cleaned = "silver.ratings_cleaned", 
    table_enriched = "silver.rating_features",

    merge_keys_cleaned = ["user_id", "movie_id", "event_timestamp"],
    merge_keys_features = ["user_id", "movie_id", "event_timestamp"],

    partition_cols_features = ["event_date"],

    run_features = False
)