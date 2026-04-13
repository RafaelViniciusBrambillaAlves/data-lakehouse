from dataclasses import dataclass
from processing.spark.core.base_config import BasePipelineConfig

@dataclass(frozen = True)
class RecommendationHistoryConfig(BasePipelineConfig):
    pass

CONFIG = RecommendationHistoryConfig(
    name = "recommendation_history",
    database = "silver",
    bronze_path = "s3a://lakehouse/bronze.db/streaming/user_recommendation_history",
    
    silver_cleaned_path = "s3a://lakehouse/silver.db/cleaned/recommendation_history",
    silver_enriched_path = "s3a://lakehouse/silver.db/enriched/recommendation_features",

    table_cleaned = "silver.recommendation_history_cleaned",
    table_enriched = "silver.recommendation_features",

    merge_keys_cleaned = ["user_id", "movie_id", "event_timestamp"],
    merge_keys_features = ["user_id", "movie_id", "event_timestamp"],
  
    partition_cols_features = ["event_date"]
)