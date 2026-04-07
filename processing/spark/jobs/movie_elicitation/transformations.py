from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType


VALID_SOURCE_TYPES = {1, 2, 3, 4, 5}

SOURCE_CATEGORY_MAP = {
    1: "popular",
    2: "well_rated",
    3: "recent_popular",
    4: "trending",
    5: "serendipity"
}

RECENCY_DAYS = 365


def build_cleaned(df: DataFrame) -> DataFrame:

    # Renomeando colunas
    df = (
        df
        .withColumnRenamed("tstamp", "event_timestamp")
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")
        .withColumnRenamed("source", "source_type")
        .withColumnRenamed("_source", "source_system")
    )

    # Filtrando
    df = df.filter(
        F.col("movie_id").isNotNull() &
        F.col("event_timestamp").isNotNull()
    )

    # Validação tipo de dominio
    df = df.withColumn(
        "source_type",
        F.when(
            F.col("source_type").isin(*VALID_SOURCE_TYPES),
            F.col("source_type")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # Validacao month_idx
    df = df.withColumn(
        "month_idx",
        F.when(
            F.col("month_idx").isNotNull() & (F.col("month_idx") >= 0),
            F.col("month_idx")
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # Dedup
    df = df.dropDuplicates(["movie_id", "event_timestamp"])

    # Schema final
    df = df.select(
        F.col("movie_id").cast(IntegerType()),
        F.col("month_idx").cast(IntegerType()),
        F.col("source_type").cast(IntegerType()),
        F.col("event_timestamp").cast(TimestampType()),
        F.col("ingestion_timestamp").cast(TimestampType()),
        F.col("source_system").cast(StringType()),
        F.current_timestamp().alias("processed_timestamp")
    )

    return df


def build_features(df: DataFrame) -> DataFrame:

    # 
    source_category_expr = F.when(F.lit(False), F.lit(None))

    for code, label in SOURCE_CATEGORY_MAP.items():
        source_category_expr = source_category_expr.when(
            F.col("source_type") == code, F.lit(label)
        )

    source_category_expr = source_category_expr.otherwise(
        F.lit(None).cast(StringType())
    )

    df = (
        df
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("source_category", source_category_expr)
        .withColumn(
            "month_group",
            F.when(F.col("month_idx").isNull(), "unknown")
             .when(F.col("month_idx").between(0, 2), "early_stage")
             .when(F.col("month_idx").between(3, 6), "mid_stage")
             .otherwise("late_stage")
        )
        .withColumn(
            "is_recent",
            F.col("event_timestamp") >= (
                F.current_timestamp() - F.expr(f"INTERVAL {RECENCY_DAYS} DAYS")
            )
        )
        .select(
            "movie_id",
            "month_idx",
            "source_type",
            "source_category",
            "month_group",
            "event_timestamp",
            "event_date",
            "is_recent"
        )
    )

    return df