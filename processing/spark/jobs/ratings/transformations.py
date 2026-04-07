from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType

# CONSTANTS
MIN_RATING = 0.5
MAX_RATING = 5.0
POSITIVE_RATING_THRESHOLD = 4.0

RATING_BUCKETS = [
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
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("_source", "source_system")
        .withColumnRenamed("_topic", "source_topic")
    )

    df = (
        df
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("movie_id", F.col("movie_id").cast(IntegerType()))
        .withColumn("event_timestamp", F.col("event_timestamp").cast(TimestampType()))
    )

    df = (
        df
        .withColumn("_rating_dbl", F.col("rating").cast(DoubleType()))
        .withColumn(
            "rating",
            F.when(F.col("_rating_dbl").between(MIN_RATING, MAX_RATING), F.col("_rating_dbl"))
             .otherwise(F.lit(None).cast(DoubleType()))
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
        "rating",
        "event_timestamp",
        "ingestion_timestamp",
        "source_system",
        "source_topic",
        F.current_timestamp().alias("processed_timestamp")
    )


# FEATURES
def build_features(df: DataFrame) -> DataFrame:

    rating_bucket_expr = F.when(F.col("rating").isNull(), None)

    prev = MIN_RATING
    for upper, label in RATING_BUCKETS:
        rating_bucket_expr = rating_bucket_expr.when(
            F.col("rating").between(prev, upper),
            F.lit(label)
        )
        prev = upper

    rating_bucket_expr = rating_bucket_expr.otherwise(None)

    return (
        df
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn(
            "rating_category",
            F.when(F.col("rating") <= 2.5, "low")
             .when(F.col("rating") <= 3.5, "medium")
             .otherwise("high")
        )
        .withColumn(
            "is_positive",
            F.col("rating") >= POSITIVE_RATING_THRESHOLD
        )
        .withColumn("rating_bucket", rating_bucket_expr)
        .withColumn(
            "recency_days",
            F.datediff(F.current_date(), F.col("event_timestamp"))
        )
        .select(
            "user_id",
            "movie_id",
            "rating",
            "rating_category",
            "is_positive",
            "rating_bucket",
            "event_timestamp",
            "event_date",
            "recency_days"
        )
    )