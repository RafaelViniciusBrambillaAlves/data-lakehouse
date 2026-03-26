import os 
from dotenv import load_dotenv

load_dotenv(dotenv_path = "/opt/spark/app/.env")

class Settings:

    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    POSTGRES_TABLES = os.getenv("POSTGRES_TABLES").split(',')

    
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

settings = Settings()


