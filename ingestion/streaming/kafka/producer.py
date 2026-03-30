import csv
import json
import os
import time
from pathlib import Path
from typing import List

from kafka import KafkaProducer

from config.settings import settings
from ingestion.streaming.kafka.schema_registry import get_or_register_schema
from utils.logger import get_logger
from itertools import islice

logger = get_logger("kafka-producer")


# =========================
# KAFKA
# =========================
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers = settings.KAFKA_BOOTSTRAP, 
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        linger_ms = 10,
        batch_size = 16384,
        acks = "all"
    )


# =========================
# DLQ
# =========================
def send_to_dlq(producer: KafkaProducer, row: dict, error: Exception) -> None:
    payload = {
        "data": row,
        "error": str(error),
        "timestamp": time.time()
    }
    producer.send(settings.KAFKA_TOPIC_DLQ, payload)


# =========================
# SEND COM RETRY
# =========================
def send_with_retry(
        producer: KafkaProducer, topic: str, row: dict
) -> bool:
    for attempt in range(settings.PRODUCER_RETRIES):

        try:
            producer.send(topic, row)

            return True
        
        except Exception as e:

            logger.warning(f"Erro ao enviar para '{topic}' (tentativa {attempt + 1}): {e}")

            time.sleep(settings.PRODUCER_RETRY_DELAY)

    return False


# =========================
# OFFSETS
# =========================
def load_offsets() -> dict:
    
    path = Path(settings.PROCESSED_FILES_PATH)

    if not path.exists():
        return {}

    with open(path, "r") as f:
        return json.load(f)
    
def save_offsets(offsets: dict) -> None:

    path = Path(settings.PROCESSED_FILES_PATH)

    path.parent.mkdir(parents = True, exist_ok = True)
    
    with open(path, "w") as f:
        json.dump(offsets, f, indent = 2)


# =========================
# PROCESSAMENTO
# =========================
def _topic_for_file(stem: str) -> str:
    return f"{settings.KAFKA_TOPIC_PREFIX}-{stem}"

def process_file(
    file_path: str,
    stem: str,
    producer: KafkaProducer,
    offsets: dict
) -> None:
    """
    Processa um único CSV de forma incremental
    """

    # topic = _topic_for_file(stem)
  
    last_offset = offsets.get(file_path, 0)

    # Garantir que o schema esteja registrado antes de produzir qualquer linha
    canonical, columns = get_or_register_schema(
        file_path = file_path,
        schema_name = stem,
        registry_path = settings.SCHEMA_REGISTRY_PATH
    ) 

    topic = _topic_for_file(canonical)

    logger.info(
        f"Iniciando '{file_path}' -> tópico '{topic}' | "
        f"offset: {last_offset} | colunas: {columns}"
    )

    current_line = 0
    success_count = 0

    with open(file_path, mode = "r", encoding = "utf-8") as f:
        reader = csv.DictReader(f)

        for row in islice(reader, last_offset, None):
            current_line += 1

            if current_line <= last_offset:
                continue
                
            ok = send_with_retry(producer, topic, dict(row))

            if ok:
                success_count += 1
                offsets[file_path] = current_line
            else:
                send_to_dlq(producer, dict(row), RuntimeError("Max retries exceeded"))
                logger.error(f"Linha {current_line} de '{file_path}' enviada para DLQ.")


    producer.flush()
    # save_offsets(offsets)
    logger.info(f"'{stem}': {success_count} novas linhas enviadas para '{topic}'.")

def discover_csv_files(base_path: str) -> List[Path]:
    return sorted(Path(base_path).glob("*.csv"))


# =========================
# MAIN LOOP
# =========================
def main() -> None:
    logger.info("Iniciando Kafka Producer (autodiscovery)...")

    producer = create_producer()

    while True:
        try:
            csv_files = discover_csv_files(settings.CSV_PATH)

            if not csv_files:
                logger.warning(f"Nenhum CSV encontrado em '{settings.CSV_PATH}'. Aguardando...")

                time.sleep(5)
                continue
                
            offsets = load_offsets()

            for file_path in csv_files:
                process_file(
                    file_path = str(file_path),
                    stem = file_path.stem,
                    producer = producer,
                    offsets = offsets
                )

            save_offsets(offsets)

            time.sleep(5)

        except Exception as e:
            logger.error(f"Erro no loop principal: {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    main()