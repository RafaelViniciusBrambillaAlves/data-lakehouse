from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import json
from pathlib import Path

from config.settings import settings
from utils.logger import get_logger

logger = get_logger("streaming")


# =========================
# SPARK
# =========================
def create_spark():
    return (
        SparkSession.builder
        .appName("KafkaToBronze")
        .enableHiveSupport()
        .config("spark.hadoop.fs.s3a.endpoint", settings.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# =========================
# SCHEMA REGISTRY
# =========================
def load_schemas() -> dict:
    path = Path(settings.SCHEMA_REGISTRY_PATH)

    if not path.exists():
        logger.warning("schemas.json não encontrado. Retornando registry vazio.")
        return {}

    with open(path, "r") as f:
        raw = json.load(f)

    schemas_raw: dict = raw.get("schemas", raw)

    schemas: dict = {}

    for name, cols in schemas_raw.items():
        schema = StructType()

        for c in cols:
            schema = schema.add(c, StringType())
            
        schemas[name] = schema

    logger.debug(f"Schemas carregados: {list(schemas.keys())}")
    return schemas


# =========================
# EXTRAI schema_name DO TÓPICO
# =========================
def _schema_name_from_topic(topic: str) -> str:
    prefix = f"{settings.KAFKA_TOPIC_PREFIX}-"
    if topic.startswith(prefix):
        return topic[len(prefix):]
    return topic


def _register_hive_table(spark: SparkSession, schema_name: str, path: str, batch_id: int) -> None:
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze.{schema_name}
            USING DELTA
            LOCATION '{path}'
        """)
        
        spark.sql(f"MSCK REPAIR TABLE bronze.{schema_name}")

        logger.info(f"[batch {batch_id}] Tabela bronze.{schema_name} registrada/atualizada no Hive")
    
    except Exception as e:

        logger.warning(f"[batch {batch_id}] Falha ao registrar bronze.{schema_name} no Hive: {e}")


# =========================
# PROCESSAMENTO
# =========================
def process_batch(df: DataFrame, batch_id: int, spark: SparkSession) -> None:

    if df.isEmpty():
        return

    schemas = load_schemas()

    topics = [r["topic"] for r in df.select("topic").distinct().collect()]

    for topic in topics:

        schema_name = _schema_name_from_topic(topic)
        schema = schemas.get(schema_name)

        if not schema:
            logger.warning(
                f"[batch {batch_id}] Schema não encontrado para '{schema_name}'. "
                f"Verifique se o producer já registrou o schema em schemas.json."
            )
            continue

        topic_df = df.filter(F.col("topic") == topic)

        parsed = (
            topic_df
            .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
            .select("data.*")
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source", F.lit("kafka"))
            .withColumn("_topic", F.lit(topic))
        )

        path = f"{settings.BRONZE_BASE_PATH}/{schema_name}"

        (
            parsed.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(path)
        )

        logger.info(f"[batch {batch_id}] {topic} -> {path}")

        _register_hive_table(spark, schema_name, path, batch_id)


# =========================
# MAIN
# =========================
def main():

    spark = create_spark()

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
        .option("subscribePattern", "csv-.*")
        .option("startingOffsets", "earliest")
        .load()
    )

    df = df.select("topic", "value")

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    # schemas não é mais passado como argumento — process_batch recarrega do disco
    query = (
        df.writeStream
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, spark))
        .option("checkpointLocation", f"{settings.CHECKPOINT_BASE_PATH}/bronze")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()