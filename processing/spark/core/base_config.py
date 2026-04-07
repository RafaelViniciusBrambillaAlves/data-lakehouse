from dataclasses import dataclass
from typing import Optional, List

@dataclass(frozen = True)
class BasePipelineConfig:
    name: str
    database: str
    bronze_path: str

    silver_cleaned_path: str
    silver_enriched_path: str

    table_cleaned: str
    table_enriched: str

    merge_keys_cleaned: List[str]
    merge_keys_features: List[str]

    partition_cols_cleaned: Optional[List[str]] = None
    partition_cols_features: Optional[List[str]] = None

    run_cleaned: bool = True
    run_features: bool = True