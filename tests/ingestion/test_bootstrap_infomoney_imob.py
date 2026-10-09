"""Partição inicial do IMOB: a staging antiga entra no merge como a ingestão mais
antiga, no formato da conversão nova, sem sobrescrever nada."""

import json
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.layout import TIMEZONE
from ingestion.storage import SUCCESS_MARKER, LocalStorageBackend, StorageFactory

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "ingestion"))

import bootstrap_infomoney_imob as bootstrap  # type: ignore[import-not-found]  # noqa: E402

OLD = pa.table(
    {
        "symbol": ["IMOB.SA", "IMOB.SA"],
        "data_pregao": ["2022-12-30", "2026-08-31"],
        "open": ["1000.1", "1293.78"],
        "high": ["1001.2", "1293.78"],
        "low": ["999.3", "1293.78"],
        "close": ["1000.4", "1293.78"],
        "volume": ["0", "0"],
        "dt_ingest": ["2026-05-07T10:00:00", "2026-09-01T14:44:15.326096"],
    }
)


@pytest.fixture
def lake(tmp_path: Path) -> LocalStorageBackend:
    storage = StorageFactory.create("local", root=tmp_path / "lake")
    assert isinstance(storage, LocalStorageBackend)
    local = tmp_path / "old.parquet"
    pq.write_table(OLD, local)
    storage.put_file(bootstrap.OLD_KEY, local)
    return storage


def test_writes_one_partition_dated_by_the_last_old_ingestion(
    lake: LocalStorageBackend, tmp_path: Path
) -> None:
    prefix = bootstrap.bootstrap(lake, lake, tmp_path / "work", executar=True)

    # 14:44 de 01/09 no dt_ingest antigo (horário de Brasília): antes de qualquer
    # ingestão nova, então o dado novo vence no merge.
    assert prefix == "staging/infomoney/acoes_imob/2026-09-01/144415/"
    assert lake.list(prefix) == [prefix + "IMOB.SA.parquet", prefix + SUCCESS_MARKER]
    table = pq.read_table(lake.root / prefix / "IMOB.SA.parquet")
    assert table.column_names == [
        "data_pregao",
        "1. open",
        "2. high",
        "3. low",
        "4. close",
        "5. volume",
    ]
    assert table.column("4. close").to_pylist() == ["1000.4", "1293.78"]
    manifest = json.loads((lake.root / prefix / SUCCESS_MARKER).read_text())
    assert manifest["files"][0]["origem"] == bootstrap.OLD_KEY


def test_dry_run_writes_nothing(lake: LocalStorageBackend, tmp_path: Path) -> None:
    prefix = bootstrap.bootstrap(lake, lake, tmp_path / "work", executar=False)

    assert lake.list(prefix) == []


def test_existing_partition_is_never_overwritten(
    lake: LocalStorageBackend, tmp_path: Path
) -> None:
    bootstrap.bootstrap(lake, lake, tmp_path / "work", executar=True)

    with pytest.raises(FileExistsError, match="já existe"):
        bootstrap.bootstrap(lake, lake, tmp_path / "work2", executar=True)


def test_partition_time_is_brasilia() -> None:
    when = bootstrap.last_ingestion(OLD)

    assert when == datetime(2026, 9, 1, 14, 44, 15, 326096, tzinfo=TIMEZONE)
