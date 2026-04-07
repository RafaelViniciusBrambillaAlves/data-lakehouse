from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType

# CONSTANTS
MIN_PREDICTED_RATING = 0.0
MAX_PREDICTED_RATING = 5.0
HIGH_SCORE_THRESHOLD = 4.0

PREDICTED_RATING_BUCKETS = [
    (1.5, "very_low"),
    (2.5, "low"),
    (3.5, "mid"),
    (4.5, "high"),
    (5.0, "top")
]


# CLEANED
def build_cleaned(df: DataFrame) -> DataFrame:

    df = (
        df
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("movieId", "movie_id")
        .withColumnRenamed("predictedRating", "predicted_rating")
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("_source", "source_system")
        .withColumnRenamed("_topic", "source_topic")
    )

    df = (
        df
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp").cast(DoubleType()))
        )
    )

    df = (
        df
        .withColumn("_rating_dbl", F.col("predicted_rating").cast(DoubleType()))
        .withColumn(
            "predicted_rating",
            F.when(
                F.col("_rating_dbl").between(MIN_PREDICTED_RATING, MAX_PREDICTED_RATING),
                F.col("_rating_dbl")
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .drop("_rating_dbl")
    )

    df = df.filter(
        F.col("user_id").isNotNull() &
        F.col("movie_id").isNotNull() &
        F.col("event_timestamp").isNotNull()
    )

    df = df.dropDuplicates(["user_id", "movie_id", "event_timestamp"])

    return df.select(
        "user_id",
        "movie_id",
        "predicted_rating",
        "event_timestamp",
        "ingestion_timestamp",
        "source_system",
        "source_topic",
        F.current_timestamp().alias("processed_timestamp")
    )


# FEATURES
def build_features(df: DataFrame) -> DataFrame:

    bucket_expr = F.when(F.col("predicted_rating").isNull(), None)

    prev = MIN_PREDICTED_RATING
    for upper, label in PREDICTED_RATING_BUCKETS:
        bucket_expr = bucket_expr.when(
            F.col("predicted_rating").between(prev, upper),
            F.lit(label)
        )
        prev = upper

    bucket_expr = bucket_expr.otherwise(None)

    return (
        df
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn(
            "predicted_rating_category",
            F.when(F.col("predicted_rating") <= 2.5, "low")
             .when(F.col("predicted_rating") <= 3.5, "medium")
             .otherwise("high")
        )
        .withColumn(
            "is_high_score",
            F.col("predicted_rating") >= HIGH_SCORE_THRESHOLD
        )
        .withColumn("predicted_rating_bucket", bucket_expr)
        .select(
            "user_id",
            "movie_id",
            "predicted_rating",
            "predicted_rating_category",
            "is_high_score",
            "predicted_rating_bucket",
            "event_timestamp",
            "event_date"
        )
    )