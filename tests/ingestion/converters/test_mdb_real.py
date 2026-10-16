"""Conversor de mdb contra o mdbtools e um arquivo Access reais.

Pulado sem `INGESTION_TEST_MDB` (caminho de um .mdb/.accdb local) ou sem o
mdbtools no PATH. O arquivo de teste vem do lake e pode ter dado pessoal: nunca
entra no repositório.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConverterConfig, ConverterFactory
from ingestion.layout import safe_segment

pytestmark = pytest.mark.integration


@pytest.fixture
def real_mdb() -> Path:
    path = Path(os.environ.get("INGESTION_TEST_MDB", ""))
    if not path.is_file():
        pytest.skip("INGESTION_TEST_MDB não aponta para um arquivo")
    if shutil.which("mdb-export") is None or shutil.which("mdb-count") is None:
        pytest.skip("mdbtools não está no PATH")
    return path


def _mdbtools(*args: str) -> str:
    return subprocess.run(
        list(args),
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"},
    ).stdout


def test_every_table_becomes_text_parquet_with_all_rows(
    real_mdb: Path, tmp_path: Path
) -> None:
    tables = [t for t in _mdbtools("mdb-tables", "-1", str(real_mdb)).splitlines() if t]
    by_output = {
        f"{safe_segment(real_mdb.stem)}__{safe_segment(t)}.parquet": t for t in tables
    }
    converter = ConverterFactory.for_file(real_mdb, ConverterConfig())

    converted = list(converter.convert(real_mdb, tmp_path / "out"))

    assert sorted(item.name for item in converted) == sorted(by_output)
    for item in converted:
        table = by_output[item.name]
        expected = int(_mdbtools("mdb-count", str(real_mdb), table).strip())
        assert item.rows == expected, table
        assert all(
            field.type == pa.string() for field in pq.read_schema(item.path)
        ), table
