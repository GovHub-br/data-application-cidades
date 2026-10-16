"""Storage montado em runtime a partir do ambiente (nunca no parse da DAG)."""

from pathlib import Path

import pytest

from ingestion.storage import (
    LocalStorageBackend,
    PrefixedStorage,
    S3StorageBackend,
    storage_from_env,
)


def test_defaults_to_minio_bucket_through_minio_lake_connection() -> None:
    backend = storage_from_env({"MINIO_BUCKET": "data-lake-mcid"})

    assert isinstance(backend, S3StorageBackend)
    assert backend.bucket == "data-lake-mcid"
    assert backend.conn_id == "minio_lake"


def test_connection_id_can_be_overridden() -> None:
    backend = storage_from_env(
        {"MINIO_BUCKET": "data-lake-mcid", "INGESTION_STORAGE_CONN_ID": "minio_homolog"}
    )

    assert isinstance(backend, S3StorageBackend)
    assert backend.conn_id == "minio_homolog"


def test_local_backend_uses_local_root(tmp_path: Path) -> None:
    backend = storage_from_env(
        {"INGESTION_STORAGE_BACKEND": "local", "INGESTION_LOCAL_ROOT": str(tmp_path)}
    )

    assert isinstance(backend, LocalStorageBackend)
    assert backend.root == tmp_path.resolve()


def test_s3_without_bucket_is_an_error() -> None:
    # Sem bucket explícito não há default: errar de bucket grava no lake de outro time.
    with pytest.raises(ValueError, match="MINIO_BUCKET"):
        storage_from_env({})


def test_local_without_root_is_an_error() -> None:
    with pytest.raises(ValueError, match="INGESTION_LOCAL_ROOT"):
        storage_from_env({"INGESTION_STORAGE_BACKEND": "local"})


def test_unknown_backend_lists_registered_ones() -> None:
    with pytest.raises(ValueError, match="local, s3"):
        storage_from_env({"INGESTION_STORAGE_BACKEND": "ftp"})


def test_prefix_wraps_the_backend(tmp_path: Path) -> None:
    backend = storage_from_env(
        {
            "INGESTION_STORAGE_BACKEND": "local",
            "INGESTION_LOCAL_ROOT": str(tmp_path),
            "INGESTION_STORAGE_PREFIX": "tests",
        }
    )

    assert isinstance(backend, PrefixedStorage)
    assert backend.prefix == "tests/"
    source = tmp_path / "f.txt"
    source.write_text("x")
    backend.put_file("raw/a/b.txt", source)
    assert (tmp_path / "tests" / "raw" / "a" / "b.txt").exists()
    assert backend.list("raw/") == ["raw/a/b.txt"]


@pytest.mark.parametrize("prefix", ["raw/", "staging", "staging/algo/"])
def test_prefix_cannot_be_the_real_lake_layers(tmp_path: Path, prefix: str) -> None:
    with pytest.raises(ValueError, match="prefixo"):
        storage_from_env(
            {
                "INGESTION_STORAGE_BACKEND": "local",
                "INGESTION_LOCAL_ROOT": str(tmp_path),
                "INGESTION_STORAGE_PREFIX": prefix,
            }
        )
