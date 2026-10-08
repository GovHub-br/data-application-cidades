"""Storage do lake (MinIO em produção, disco local nos testes).

Toda operação trabalha com arquivo em disco, nunca com o dataset em memória.
"""

from ingestion.storage.base_storage import StorageBackend
from ingestion.storage.config_storage import storage_from_env
from ingestion.storage.landing import SUCCESS_MARKER, LandingResult, land
from ingestion.storage.models.local_storage import LocalStorageBackend
from ingestion.storage.models.s3_storage import S3StorageBackend
from ingestion.storage.storage_errors import ObjectNotFoundError, StorageError
from ingestion.storage.storage_registry import StorageFactory

__all__ = [
    "SUCCESS_MARKER",
    "LandingResult",
    "LocalStorageBackend",
    "ObjectNotFoundError",
    "S3StorageBackend",
    "StorageBackend",
    "StorageError",
    "StorageFactory",
    "land",
    "storage_from_env",
]
