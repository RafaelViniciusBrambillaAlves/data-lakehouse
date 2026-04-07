from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType

from utils.logger import get_logger

logger = get_logger("belief-transformations")

SENTINEL_VALUE = -1.0
HIGH_CONFIDENCE_THRESHOLD = 4.0


# =============================
# CLEANED
# =============================
def build_cleaned(df: DataFrame) -> DataFrame:

    logger.info("Iniciando transformação CLEANED")

    # Rename columns
    df = (
        df
        .withColumnRenamed("user_elicit_rating", "user_rating")
        .withColumnRenamed("user_predict_rating", "predicted_rating")
        .withColumnRenamed("system_predict_rating", "system_rating")
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("_source", "source_system")
    )

    # Sentinel handling
    df = (
        df
        .withColumn(
            "is_seen",
            F.when(F.col("is_seen") == -1, None).otherwise(F.col("is_seen"))
        )
        .withColumn(
            "user_rating",
            F.when(F.col("user_rating") == SENTINEL_VALUE, None).otherwise(F.col("user_rating"))
        )
        .withColumn(
            "predicted_rating",
            F.when(F.col("predicted_rating") == SENTINEL_VALUE, None).otherwise(F.col("predicted_rating"))
        )
        .withColumn(
            "user_certainty",
            F.when(F.col("user_certainty") == SENTINEL_VALUE, None).otherwise(F.col("user_certainty"))
        )
    )

    # Date handling
    df = df.withColumn(
        "watch_date",
        F.when(
            F.col("watch_date").isNull() | (F.trim(F.col("watch_date")) == ""),
            None
        ).otherwise(F.to_date("watch_date"))
    )

    df = df.filter(
        F.col("user_id").isNotNull() & F.col("movie_id").isNotNull()
    )

    # Dedup
    df = df.dropDuplicates(["user_id", "movie_id", "event_timestamp"])

    # Final typing
    df = df.select(
        F.col("user_id").cast(IntegerType()),
        F.col("movie_id").cast(IntegerType()),
        F.col("is_seen").cast(IntegerType()),
        F.col("watch_date").cast(DateType()),
        F.col("user_rating").cast(DoubleType()),
        F.col("predicted_rating").cast(DoubleType()),
        F.col("user_certainty").cast(DoubleType()),
        F.col("system_rating").cast(DoubleType()),
        F.col("event_timestamp").cast(TimestampType()),
        F.col("movie_idx").cast(IntegerType()),
        F.col("source").cast(IntegerType()).alias("source_type"),
        F.col("ingestion_timestamp").cast(TimestampType()),
        F.col("source_system").cast(StringType()),
        F.current_timestamp().alias("processed_timestamp")
    )

    logger.info("CLEANED finalizado")

    return df


# =============================
# FEATURES
# =============================
def build_features(df: DataFrame) -> DataFrame:

    logger.info("Iniciando transformação FEATURES")

    df = (
        df
        .withColumn(
            "rating_diff",
            F.when(
                F.col("user_rating").isNotNull() & F.col("system_rating").isNotNull(),
                F.round(F.col("user_rating") - F.col("system_rating"), 4)
            )
        )
        .withColumn(
            "is_high_confidence",
            F.when(
                F.col("user_certainty").isNotNull(),
                F.col("user_certainty") >= HIGH_CONFIDENCE_THRESHOLD
            )
        )
        .withColumn("has_watched", F.col("is_seen") == 1)
        .withColumn("event_date", F.to_date("event_timestamp"))
    )

    df = df.select(
        "user_id",
        "movie_id",
        "is_seen",
        "has_watched",
        "user_rating",
        "system_rating",
        "rating_diff",
        "user_certainty",
        "is_high_confidence",
        "event_timestamp",
        "event_date"
    )

    logger.info("FEATURES finalizado")

    return df