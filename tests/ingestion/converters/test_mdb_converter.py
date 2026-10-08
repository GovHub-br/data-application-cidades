"""Conversor de mdb/accdb via mdbtools (simulado: o binário não está na máquina)."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory
from tests.ingestion.converters.conftest import fake_mdb


def _convert(path: Path, **config: Any) -> dict[str, list[dict[str, Any]]]:
    converter = ConverterFactory.for_file(path, ConverterConfig(**config))
    tables = {}
    for converted in converter.convert(path, path.parent / "out"):
        table = pq.read_table(converted.path)
        assert all(field.type == pa.string() for field in table.schema)
        tables[converted.name] = table.to_pylist()
    return tables


def test_each_table_becomes_a_file_and_values_stay_text(
    fake_mdbtools: Path, tmp_path: Path
) -> None:
    path = fake_mdb(
        tmp_path / "base.accdb",
        {
            "contratos": 'apf,valor,obs\n"0001",1500.00,"linha 1\nlinha 2"\n"0002",,""\n',
            "vazia": "apf\n",
        },
    )

    assert _convert(path) == {
        "base__contratos.parquet": [
            {"apf": "0001", "valor": "1500.00", "obs": "linha 1\nlinha 2"},
            {"apf": "0002", "valor": None, "obs": ""},
        ],
        "base__vazia.parquet": [],
    }


def test_include_filters_tables(fake_mdbtools: Path, tmp_path: Path) -> None:
    path = fake_mdb(tmp_path / "base.mdb", {"Contratos": "a\n1\n", "MSysAux": "b\n2\n"})

    assert list(_convert(path, include="^Contratos$")) == ["base__Contratos.parquet"]


def test_failing_export_is_a_conversion_error(
    fake_mdbtools: Path, tmp_path: Path
) -> None:
    # mdb-tables lista uma tabela que o mdb-export não consegue exportar.
    path = tmp_path / "quebrado.mdb"
    path.write_text('{"ok": "a\\n1\\n"}')
    (fake_mdbtools / "mdb-tables").write_text(
        (fake_mdbtools / "mdb-tables")
        .read_text()
        .replace("print(", "print('fantasma');print(")
    )

    with pytest.raises(ConversionError, match="fantasma"):
        _convert(path)


def test_missing_mdbtools_is_a_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "vazio"))
    path = tmp_path / "base.mdb"
    path.write_text("{}")

    with pytest.raises(ConversionError, match="mdbtools"):
        _convert(path)


def test_table_names_with_accents_are_exported(
    fake_mdbtools: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Achado no arquivo real (Posição_CCI_CCA): sem locale UTF-8, o mdb-export
    # recusa o nome. O conversor não pode depender do locale do container.
    monkeypatch.delenv("LC_ALL", raising=False)
    monkeypatch.setenv("LANG", "C")
    path = fake_mdb(tmp_path / "base.mdb", {"Posição_CCI_CCA": "a\n1\n"})

    assert _convert(path) == {"base__Posi__o_CCI_CCA.parquet": [{"a": "1"}]}
