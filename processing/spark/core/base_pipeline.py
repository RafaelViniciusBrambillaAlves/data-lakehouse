from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import time

from processing.spark.core.base_config import BasePipelineConfig

from processing.spark.core.reader import read_incremental
from processing.spark.core.checkpoint import (
    get_current_bronze_version,
    get_last_processed_version,
    save_checkpoint
)
from processing.spark.core.writer import write_cleaned, write_features
import logging
import sys


class BaseSilverPipeline:

    def __init__(self, spark: SparkSession, config: BasePipelineConfig, logger):
        self.spark = spark
        self.config = config
        self.logger = logger 


    def run(self):
        start_time = time.time()
        status = "FALSE"
   
        try:
            self._execute()
            status = "OK"
            
        except Exception:
            self.logger.exception("Pipeline falhou com exceção não tratada")

        finally:
            elapsed = time.time() - start_time

            self.logger.info(
                "Pipeline finalizado - status = %s | total = %.2fs | pipeline = %s",
                status,
                elapsed,
                self.config.name,
            )

            self._flush_logs()

        return {
            "status": status,
            "elapsed": elapsed,
        }


    def build_cleaned(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError


    def build_features(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError


    def _execute(self) -> None:
        
        self.logger.info(
            "Pipeline iniciado - %s",
            self.config.name,
            extra = {"pipeline": self.config.name},
        )

        t0 = time.time()
        current_version = get_current_bronze_version(self.spark, self.config)
        last_version = get_last_processed_version(self.spark, self.config)
        self.logger.info("Checkpoint load: %.2fs", time.time() - t0)

        if self._is_up_to_date(last_version, current_version):
            self.logger.info("Nenhum dado novo. Pipeline encerrado")
            return 
        
        t1 = time.time()
        df_raw = self._read(last_version)
        self.logger.info("Leitura Bronze: %.2fs", time.time() - t1)

        if df_raw.isEmpty():
            self.logger.info("Lote vazio. Pipeline encerrado")
            return 
        
        self.logger.info(
            "Processando versões bronze - last_version=%s | current_version=%s",
            last_version,
            current_version,
            extra={"last_version": last_version, "current_version": current_version},
        )

        t2 = time.time()
        self._process_and_write(df_raw, current_version)
        self.logger.info("Process + Write: %.2fs", time.time() - t2)


    def _process_and_write(self, df_raw: DataFrame, current_version: int) -> None:
        
        if self.config.run_cleaned:
            df_cleaned = self.build_cleaned(df_raw)
            write_cleaned(self.spark, df_cleaned, self.config, self.logger)
        else:
            df_cleaned = df_raw

        if self.config.run_features:
            df_features = self.build_features(df_cleaned)
            write_features(self.spark, df_features, self.config, self.logger)

        save_checkpoint(self.spark, self.config, current_version)


    def _is_up_to_date(self, last: Optional[int], current: int) -> bool:
        return last is not None and last >= current


    def _read(self, last_version: Optional[int]) -> DataFrame:
        return read_incremental(self.spark, self.config, last_version)


    @staticmethod
    def _flush_logs() -> None:

        for handler in logging.root.handlers:
            handler.flush()

        sys.stdout.flush()
        sys.stderr.flush()