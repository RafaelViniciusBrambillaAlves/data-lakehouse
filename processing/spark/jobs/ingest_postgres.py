from processing.spark.utils.spark_session import create_spark_session
from ingestion.batch.postgres.extract import extract_table
from ingestion.batch.loaders.bronze_loader import load_to_bronze
from config.settings import settings
from utils.logger import get_logger
import os
import traceback

logger = get_logger("ingestion-job")

def main():
    spark = create_spark_session("ingest-postgres")

    table = os.environ.get("TABLE_NAME")
    
    if not table:
        logger.error("Variável TABLE_NAME não definida")
        raise ValueError("TABLE_NAME não definida")
    
    logger.info(f"Tabelas para ingestão: {table}")


    try:
        df = extract_table(spark, table)

        table_name = table.split(".")[-1]

        load_to_bronze(df, table_name, spark)

    except Exception as e:
        # logger.error(f"Erro na tabela {table}: {repr(e)}")
        traceback.print_exc()
        raise

    logger.info("Ingestão finalizada")

if __name__ == "__main__":
    main()