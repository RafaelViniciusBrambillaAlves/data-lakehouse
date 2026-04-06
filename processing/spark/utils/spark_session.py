from pyspark.sql import SparkSession
import os
from config.settings import settings

def create_spark_session(app_name: str = "lakehouse_batch") -> SparkSession:

    jars_dir = "/opt/spark/jars"
    extra_jars = ",".join([
        f"{jars_dir}/delta-core_2.12-2.4.0.jar",
        f"{jars_dir}/delta-storage-2.4.0.jar",
        f"{jars_dir}/hadoop-aws-3.3.4.jar",
        f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar",
        f"{jars_dir}/postgresql-42.6.0.jar",
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)

        # Hive Support
        .enableHiveSupport() 
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        # .config("spark.sql.warehouse.dir", "s3a://gold/warehouse")

        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Classpath
        .config("spark.driver.extraClassPath", extra_jars)
        .config("spark.executor.extraClassPath", extra_jars)

        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", settings.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark