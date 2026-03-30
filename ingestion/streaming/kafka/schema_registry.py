"""
Schemas Registry local.

Inferir o schema de um CSV na primeira leitura

Convenção: 
    - Todos os campos são tratados como StringType na camada bronze.
    - A tipagem forte ocorre na camada silver
    - Um tópico Kafka é criado por schema único, não por arquivo

Estrutura do registry (schemas.json):
    {
        "schemas": {
            "<schema_name>": ["col1", "col2", ...]  
        },
        "file_mappings": {
            "<file_path>": "<schema_name>"    
        }
    }
"""

import csv 
import json 
from pathlib import Path
from typing import Optional, List, Tuple

from utils.logger import get_logger

logger = get_logger("schema-registry")

def _load_registry(registry_path: str) -> dict:
    path = Path(registry_path)

    path.parent.mkdir(parents = True, exist_ok = True)

    if not path.exists():
        return {"schemas": {}, "file_mappings": {}}
    
    with open(path, "r") as f:
        raw = json.load(f)
    
    if "schemas" not in raw:

        logger.warning("Registry em formato legado detectado. Migrando para novo formato...")
        return {"schemas": raw, "file_mappings": {}}
    
    return raw


def _save_registry(registry: dict, registry_path: str) -> None:
    path = Path(registry_path)

    path.parent.mkdir(parents = True, exist_ok = True)

    with open(path, "w") as f:
        json.dump(registry, f, indent = 2)


def _infer_columns(file_path: str) -> List[str]:
    """
    Le apenas o header do CSV e retorna a lista de colunas
    """
    with open(file_path, mode = "r", encoding = "utf-8") as f:
        reader = csv.reader(f)

        header = next(reader)
    
    columns = [col.strip() for col in header]

    logger.info(f"Colunas inferidas de '{file_path}': {columns}")

    return columns

def _find_matching_schema(schemas: dict, columns: List[str]) -> Optional[str]:
    """
    Busca um schema já registrado cujas colunas sejam idênticas as informadas
    Retorno o schema_name ou None se não houver correspondencia
    """
    for schema_name, registered_columns in schemas.items():
        if registered_columns == columns:
            return schema_name
    
    return None 


def get_or_register_schema(
        file_path: str,
        schema_name: str,
        registry_path: str,
        force_refresh: bool = False,
) -> Tuple[str, List[str]]:
    """
    Retorna o Schema (lista de colunas) para uma arquivo csv
    - Se o arquivo já foi mapeado anteriormente, reutiliza o schema_name registrado
    - Caso contrário, registra um novo schema
    """

    registry = _load_registry(registry_path)
    schemas: dict = registry["schemas"]
    file_mappings: dict = registry["file_mappings"]

    if file_path in file_mappings and not force_refresh:
        canonical = file_mappings[file_path]

        columns = schemas[canonical]

        logger.debug(f"'{file_path}' já mapeado para schema '{canonical}': {columns}")

        return canonical, columns 
    
    columns = _infer_columns(file_path)

    canonical = _find_matching_schema(schemas, columns)

    if canonical:
        logger.info(
            f"'{file_path}' reutiliza schema existente '{canonical}' "
            f"(colunas idênticas). Tópico compartilhado."
        )
    else:
        canonical = _unique_schema_name(schema_name, schemas)

        schemas[canonical] = columns

        logger.info(f"Novo schema '{canonical}' registrado com colunas: {columns}")

    file_mappings[file_path] = canonical
    _save_registry(registry, registry_path)

    return canonical, columns 


def _unique_schema_name(desired: str, schemas: dict) -> str:
    """
    Garante que o schema seja unico
    Se ja existir (com colunas diferentes) adiciona sufixo, _1, _2
    """
    if desired not in schemas:
        return desired
    
    counter = 1

    while f"{desired}_{counter}" in schemas:
        counter += 1

    unique = f"{desired}_{counter}"

    logger.warning(
        f"Schema '{desired}' já existe com colunas diferentes. "
        f"Registrando como '{unique}'."
    )
    return unique


def get_schema(schema_name: str, registry_path: str) -> Optional[List[str]]:
    """
    Retorna o Schema registrado ou None se não encontrado
    """
    registry = _load_registry(registry_path)
    return registry["schemas"].get(schema_name)


def list_schema(registry_path: str) -> dict:
    """
    Retorna todos os schemas registrados
    """
    return _load_registry(registry_path)["schemas"]

def list_file_mappings(registry_path: str) -> dict:
    return _load_registry(registry_path)["file_mappings"]