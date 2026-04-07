from dataclasses import dataclass
from processing.spark.core.base_config import BasePipelineConfig

@dataclass(frozen = True)
class MoviesConfig(BasePipelineConfig):
    pass

CONFIG = MoviesConfig(
    name = "movies",
    database = "silver",
    bronze_path = "s3a://bronze/batch/movies",

    silver_cleaned_path = "s3a://silver/cleaned/movies",
    silver_enriched_path = "s3a://silver/enriched/movie_genres",

    table_cleaned = "silver.movies_cleaned" ,
    table_enriched = "silver.movie_genres",

    merge_keys_cleaned = ["movie_id"],
    merge_keys_features = ["movie_id", "genre"],

    partition_cols_cleaned = ["release_year"],
    partition_cols_features = None
)

