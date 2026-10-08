"""Conversão de uma partição completa da raw para a staging, com publicação."""

import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterConfig, convert_partition
from ingestion.storage import SUCCESS_MARKER, StorageBackend, land
from ingestion.extractors import write_stream

RAW = "raw/bacen/sgs/2026-10-08/060000/"
STAGING = "staging/bacen/sgs/2026-10-08/060000/"
LATEST = "staging/bacen/sgs/latest/"


def _land_raw(storage: StorageBackend, tmp_path: Path, files: dict[str, bytes]) -> None:
    parts = (
        write_stream([data], tmp_path / "raw-local" / name)
        for name, data in files.items()
    )
    land(storage, parts, RAW)


def _convert(storage: StorageBackend, tmp_path: Path, **config: Any) -> object:
    return convert_partition(
        storage,
        raw_prefix=RAW,
        staging_prefix=STAGING,
        latest_prefix=LATEST,
        config=ConverterConfig(**config),
        work_dir=tmp_path / "work",
    )


def test_converts_every_raw_file_writes_manifest_and_publishes(
    lake_storage: StorageBackend, tmp_path: Path
) -> None:
    _land_raw(
        lake_storage,
        tmp_path,
        {
            "ipca.json": b'[{"data":"01/07/2026","valor":"0.26"}]',
            "selic.json": (
                b'[{"data":"08/10/2026","valor":"13.75"},'
                b'{"data":"09/10/2026","valor":"13.75"}]'
            ),
        },
    )

    _convert(lake_storage, tmp_path)

    assert lake_storage.list(STAGING) == [
        STAGING + SUCCESS_MARKER,
        STAGING + "ipca.parquet",
        STAGING + "selic.parquet",
    ]
    manifest = tmp_path / "m.json"
    lake_storage.get_file(STAGING + SUCCESS_MARKER, manifest)
    entries = {f["name"]: f for f in json.loads(manifest.read_text())["files"]}
    assert entries["selic.parquet"]["source"] == RAW + "selic.json"
    assert entries["selic.parquet"]["rows"] == 2
    assert entries["selic.parquet"]["columns"] == ["data", "valor"]
    assert lake_storage.list(LATEST) == [
        LATEST + SUCCESS_MARKER,
        LATEST + "ipca.parquet",
        LATEST + "selic.parquet",
    ]
    local = tmp_path / "selic.parquet"
    lake_storage.get_file(LATEST + "selic.parquet", local)
    assert pq.read_table(local).num_rows == 2
    assert not any(path.is_file() for path in (tmp_path / "work").rglob("*"))


def test_raw_without_success_is_not_converted(
    lake_storage: StorageBackend, tmp_path: Path
) -> None:
    local = tmp_path / "ipca.json"
    local.write_bytes(b"[]")
    lake_storage.put_file(RAW + "ipca.json", local)

    with pytest.raises(ConversionError, match="incompleta"):
        _convert(lake_storage, tmp_path)
    assert lake_storage.list(STAGING) == []


def test_failure_keeps_the_previous_latest(
    lake_storage: StorageBackend, tmp_path: Path
) -> None:
    _land_raw(lake_storage, tmp_path, {"ipca.json": b'[{"v":"1"}]'})
    _convert(lake_storage, tmp_path)
    for key in lake_storage.list(RAW):
        lake_storage.delete(key)
    _land_raw(
        lake_storage, tmp_path, {"ipca.json": b'[{"v":"2"}]', "quebrado.json": b"[{"}
    )

    with pytest.raises(ConversionError):
        _convert(lake_storage, tmp_path)

    local = tmp_path / "latest.parquet"
    lake_storage.get_file(LATEST + "ipca.parquet", local)
    assert pq.read_table(local).to_pylist() == [{"v": "1"}]


def test_two_raw_files_with_the_same_output_name_are_an_error(
    lake_storage: StorageBackend, tmp_path: Path
) -> None:
    _land_raw(
        lake_storage, tmp_path, {"serie.csv": b"a\n1\n", "serie.json": b'[{"a":"1"}]'}
    )

    with pytest.raises(ValueError, match="repetido"):
        _convert(lake_storage, tmp_path)
    assert not lake_storage.exists(STAGING + SUCCESS_MARKER)
