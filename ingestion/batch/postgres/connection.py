from config.settings import settings

def get_jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{settings.POSTGRES_HOST}:"
        f"{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
    )

def get_connection_properties() -> dict:
    return {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }