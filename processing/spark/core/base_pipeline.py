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

class BaseSilverPipeline:

    def __init__(self, spark: SparkSession, config: BasePipelineConfig, logger):
        self.spark = spark
        self.config = config
        self.logger = logger 


    def run(self):
        start_time = time.time()

        self.logger.info(
            f"Pipeline iniciado - {self.config.name}", 
            extra = {"pipeline": self.config.name}
        )

        current_version = get_current_bronze_version(self.spark, self.config)
        last_version = get_last_processed_version(self.spark, self.config)

        if self._is_up_to_date(last_version, current_version):
            self.logger.info("Nenhum dado novo. Pipeline encerrado")
            return 
        
        df_raw  = self._read(last_version)

        if df_raw.isEmpty():
            self.logger.info("Lote vazio. Pipeline encerrado")
            return 

        self.logger.info(
            f"Processando versões bronze -last_version: {last_version} - current_version: {current_version}",
            extra = {
                "last_version": last_version,
                "current_version": current_version
            }
        )
        
        self._process_and_write(df_raw, current_version)

        elapsed = time.time() - start_time
        self.logger.info(
            f"Pipeline finalizado em {elapsed:.2f}s", 
            extra = {"elapsed_seconds": round(elapsed, 2)}
        )


    # Overridable hooks

    def build_cleaned(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError
    

    def build_features(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError


    # Private helpers
    def _is_up_to_date(self, last: Optional[int], current: int) -> bool:
        return last is not None and last >= current 


    def _read(self, last_version: Optional[int]) -> Optional[DataFrame]:
        return read_incremental(self.spark, self.config, last_version)


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