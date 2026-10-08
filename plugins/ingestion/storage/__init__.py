"""Storage do lake (MinIO em produção, disco local nos testes).

Toda operação trabalha com arquivo em disco, nunca com o dataset em memória.
"""

from ingestion.storage.base_storage import StorageBackend
from ingestion.storage.models.local_storage import LocalStorageBackend
from ingestion.storage.storage_errors import ObjectNotFoundError, StorageError
from ingestion.storage.storage_registry import StorageFactory

__all__ = [
    "LocalStorageBackend",
    "ObjectNotFoundError",
    "StorageBackend",
    "StorageError",
    "StorageFactory",
]
