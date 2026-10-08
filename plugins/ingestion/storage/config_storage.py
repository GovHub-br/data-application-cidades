"""Escolha do storage pelo ambiente, lida em runtime (dentro da task, nunca no parse)."""

import os
from collections.abc import Mapping

from ingestion.storage.base_storage import StorageBackend
from ingestion.storage.models.prefixed_storage import PrefixedStorage
from ingestion.storage.storage_registry import StorageFactory


def storage_from_env(env: Mapping[str, str] | None = None) -> StorageBackend:
    """Monta o storage do lake a partir das variáveis de ambiente.

    - `INGESTION_STORAGE_BACKEND`: `s3` (padrão) ou `local`.
    - `s3`: bucket em `MINIO_BUCKET`, credencial na Connection
      `INGESTION_STORAGE_CONN_ID` (padrão `minio_lake`).
    - `local`: diretório em `INGESTION_LOCAL_ROOT`.
    - `INGESTION_STORAGE_PREFIX` (opcional): tudo vai para essa pasta do backend, por
      exemplo `tests/` para rodar o pipeline de verdade fora de `raw/` e `staging/`.

    O bucket não tem valor padrão de propósito: no MinIO compartilhado, errar de
    bucket grava no lake de outro time.
    """
    env = os.environ if env is None else env
    backend = _backend(env)
    prefix = env.get("INGESTION_STORAGE_PREFIX")
    return PrefixedStorage(backend, prefix) if prefix else backend


def _backend(env: Mapping[str, str]) -> StorageBackend:
    name = env.get("INGESTION_STORAGE_BACKEND", "s3")
    if name == "s3":
        return StorageFactory.create(
            "s3",
            bucket=_required(env, "MINIO_BUCKET"),
            conn_id=env.get("INGESTION_STORAGE_CONN_ID", "minio_lake"),
        )
    if name == "local":
        return StorageFactory.create("local", root=_required(env, "INGESTION_LOCAL_ROOT"))
    return StorageFactory.create(name)


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ValueError(f"variável de ambiente obrigatória ausente: {name}")
    return value
