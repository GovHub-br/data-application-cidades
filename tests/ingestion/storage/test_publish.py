"""Publicação da última ingestão completa em `latest/`, que é o que o bronze lê."""

from pathlib import Path

import pytest

from ingestion.storage import (
    SUCCESS_MARKER,
    StorageBackend,
    StorageError,
    StorageFactory,
    publish_latest,
)

STAGING = "staging/bacen/sgs/"
OLD = STAGING + "2026-10-01/060000/"
NEW = STAGING + "2026-10-08/060000/"
LATEST = STAGING + "latest/"
# O latest/ guarda a cópia com a partição de origem: o caminho que o bronze lê traz
# a data da ingestão (`dt_ingest` na prata).
LATEST_NEW = LATEST + "2026-10-08/060000/"


def _put(storage: StorageBackend, tmp_path: Path, key: str, data: bytes) -> None:
    local = tmp_path / "src" / key
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)
    storage.put_file(key, local)


def _read(storage: StorageBackend, tmp_path: Path, key: str) -> bytes:
    local = tmp_path / "read" / key
    storage.get_file(key, local)
    return local.read_bytes()


def test_partition_without_success_is_not_published(
    lake_storage: StorageBackend, tmp_path: Path
) -> None:
    _put(lake_storage, tmp_path, NEW + "ipca.parquet", b"novo")

    with pytest.raises(StorageError, match="_SUCCESS"):
        publish_latest(lake_storage, NEW, LATEST)
    assert lake_storage.list(LATEST) == []


def test_latest_mirrors_the_new_partition_and_drops_what_left(
    lake_storage: StorageBackend, tmp_path: Path
) -> None:
    for key, data in {
        OLD + "ipca.parquet": b"velho",
        OLD + "serie_extinta.parquet": b"velho",
        OLD + SUCCESS_MARKER: b"{}",
        NEW + "ipca.parquet": b"novo",
        NEW + "selic.parquet": b"novo",
        NEW + SUCCESS_MARKER: b'{"files": []}',
    }.items():
        _put(lake_storage, tmp_path, key, data)
    publish_latest(lake_storage, OLD, LATEST)

    keys = publish_latest(lake_storage, NEW, LATEST)

    assert keys == [LATEST_NEW + "ipca.parquet", LATEST_NEW + "selic.parquet"]
    assert lake_storage.list(LATEST) == [
        LATEST_NEW + "ipca.parquet",
        LATEST_NEW + "selic.parquet",
        LATEST + SUCCESS_MARKER,
    ]
    assert _read(lake_storage, tmp_path, LATEST_NEW + "ipca.parquet") == b"novo"
    assert _read(lake_storage, tmp_path, LATEST + SUCCESS_MARKER) == b'{"files": []}'
    assert lake_storage.exists(OLD + "serie_extinta.parquet")


def test_marker_is_copied_last_and_stale_files_removed_after_copies(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    local = StorageFactory.create("local", root=tmp_path / "lake")

    class Spy(StorageBackend):
        def put_file(self, key: str, local_path: Path) -> None:
            local.put_file(key, local_path)

        def get_file(self, key: str, local_path: Path) -> None:
            local.get_file(key, local_path)

        def list(self, prefix: str) -> list[str]:
            return local.list(prefix)

        def delete(self, key: str) -> None:
            calls.append(f"delete {key}")
            local.delete(key)

        def copy(self, src_key: str, dst_key: str) -> None:
            calls.append(f"copy {dst_key}")
            local.copy(src_key, dst_key)

        def exists(self, key: str) -> bool:
            return local.exists(key)

    for key in [LATEST + "antigo.parquet", NEW + "a.parquet", NEW + SUCCESS_MARKER]:
        _put(local, tmp_path, key, b"x")

    publish_latest(Spy(), NEW, LATEST)

    assert calls == [
        f"copy {LATEST_NEW}a.parquet",
        f"delete {LATEST}antigo.parquet",
        f"copy {LATEST}{SUCCESS_MARKER}",
    ]
