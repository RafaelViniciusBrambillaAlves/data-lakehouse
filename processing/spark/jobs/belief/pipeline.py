from pyspark.sql import DataFrame
from processing.spark.core.base_pipeline import BaseSilverPipeline
from processing.spark.jobs.belief.transformations import build_cleaned, build_features


class BeliefPipeline(BaseSilverPipeline):

    def build_cleaned(self, df: DataFrame) -> DataFrame:
        return build_cleaned(df)
    
    def build_features(self, df: DataFrame) -> DataFrame:
        return build_features(df)