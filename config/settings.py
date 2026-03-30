import os 
from dotenv import load_dotenv

load_dotenv(override = False)

class Settings:

    # =========================
    # POSTGRES
    # ========================= 
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_CONTAINER_NAME = os.getenv("POSTGRES_CONTAINER_NAME")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_TABLES = os.getenv("POSTGRES_TABLES", "")
    POSTGRES_TABLES = POSTGRES_TABLES.split(",") if POSTGRES_TABLES else []

    # =========================
    # MINIO
    # =========================
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    # =========================
    # DELTA LAKE
    # =========================
    BRONZE_BASE_PATH = os.getenv("BRONZE_BASE_PATH")
    CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_BASE_PATH")

    # =========================
    # KAFKA
    # =========================
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ")

    # Prefixo usado para nomear tópicos
    KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "csv")
    
    KAFKA_TOPIC_PATTERN = os.getenv("KAFKA_TOPIC_PATTERN")

    PRODUCER_RETRIES = int(os.getenv("PRODUCER_RETRIES", 3))
    PRODUCER_RETRY_DELAY = int(os.getenv("PRODUCER_RETRY_DELAY", 2))

    # =========================
    # FILESYSTEM
    # =========================
    CSV_PATH = os.getenv("CSV_PATH")
    PROCESSED_FILES_PATH = os.getenv("PROCESSED_FILES_PATH")
    SCHEMA_REGISTRY_PATH = os.getenv("SCHEMA_REGISTRY_PATH")

    # =========================
    # PROJECT ROOT
    # =========================
    PROJECT_ROOT = os.getenv("PROJECT_ROOT")

settings = Settings()
