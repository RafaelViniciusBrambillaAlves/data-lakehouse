from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

TITLE_YEAR_PATTERN = r"^(.*)\s\(\d{4}\)$"
YEAR_PATTERN = r"\((\d{4})\)"

MIN_VALID_YEAR = 1888
MAX_VALID_YEAR = 2028

def build_cleaned(df: DataFrame) -> DataFrame:

    df = df.withColumnRenamed("genres", "genres_raw")

    df = (
        df
        .withColumn("release_year", F.regexp_extract("title", YEAR_PATTERN, 1).cast(IntegerType()))
        .withColumn("title", F.trim(F.regexp_extract("title", TITLE_YEAR_PATTERN, 1)))
    )

    df = df.filter(F.col("title").isNotNull() & (F.trim("title") != ""))

    df = df.filter(
        F.col("release_year").isNotNull()
        & F.col("release_year").between(MIN_VALID_YEAR, MAX_VALID_YEAR)
    )

    df = df.fillna({"genres_raw": "unknown"})

    df = df.dropDuplicates(["movie_id"])

    return df.select(
        F.col("movie_id").cast(IntegerType()),
        F.col("title").cast(StringType()),
        F.col("release_year"),
        F.col("genres_raw"),
        F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
        F.col("_source").alias("source"),
        F.current_timestamp().alias("processed_timestamp")
    )


def build_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn("genre", F.explode(F.split("genres_raw", r"\|")))
        .withColumn("genre", F.lower(F.trim("genre")))
        .filter(
            F.col("genre").isNotNull()
            & (F.col("genre") != "")
            & (F.col("genre") != "(no genres listed)")
        )
        .select("movie_id", "genre")
        .distinct()
    )