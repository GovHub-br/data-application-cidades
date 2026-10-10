"""Parquet passa com os tipos; zip delega cada membro ao conversor da extensão."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from openpyxl import Workbook

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory
from tests.ingestion.converters.conftest import zip_of


def _convert(path: Path, **config: Any) -> dict[str, pa.Table]:
    converter = ConverterFactory.for_file(path, ConverterConfig(**config))
    return {
        converted.name: pq.read_table(converted.path)
        for converted in converter.convert(path, path.parent / "out")
    }


def test_parquet_keeps_schema_and_values(tmp_path: Path) -> None:
    original = pa.table({"ano": pa.array([2025, 2026], pa.int64()), "v": [1.5, None]})
    path = tmp_path / "hist.parquet"
    pq.write_table(original, path, row_group_size=1)

    [(name, table)] = _convert(path).items()

    assert name == "hist.parquet"
    assert table.equals(original)


def test_zip_member_uses_the_configured_structure(tmp_path: Path) -> None:
    # O relatório do Tesouro: TSV UTF-16 com 12 linhas antes do cabeçalho, num zip.
    preamble = "".join(f"linha de título {n}\n" for n in range(12))
    tsv = (preamble + "Unidade\tDotação\n53000\t1.234,56\n").encode("utf-16")
    path = tmp_path / "dotacao_execucao.zip"
    path.write_bytes(zip_of({"relatorios/Dotação.csv": tsv}))

    tables = _convert(path, encoding="utf-16", delimiter="\t", skip_rows=12)

    assert {name: t.to_pylist() for name, t in tables.items()} == {
        "dotacao_execucao__relatorios_Dota__o.parquet": [
            {"Unidade": "53000", "Dotação": "1.234,56"}
        ]
    }


def test_zip_with_xlsx_member_keeps_sheet_suffix(tmp_path: Path) -> None:
    workbook = Workbook()
    workbook.remove(workbook.worksheets[0])
    for title in ["A", "B"]:
        workbook.create_sheet(title).append([title.lower()])
    xlsx = tmp_path / "planilha.xlsx"
    workbook.save(xlsx)
    path = tmp_path / "pacote.zip"
    path.write_bytes(zip_of({"planilha.xlsx": xlsx.read_bytes()}))

    assert sorted(_convert(path)) == [
        "pacote__planilha__A.parquet",
        "pacote__planilha__B.parquet",
    ]


def test_zip_include_selects_members_and_skips_directories(tmp_path: Path) -> None:
    path = tmp_path / "pacote.zip"
    path.write_bytes(
        zip_of({"dados/a.csv": b"x\n1\n", "dados/": b"", "leia-me.txt": b"oi"})
    )

    assert list(_convert(path, include=r"\.csv$")) == ["pacote__dados_a.parquet"]


def test_zip_without_members_to_convert_is_an_error(tmp_path: Path) -> None:
    path = tmp_path / "vazio.zip"
    path.write_bytes(zip_of({"leia-me.txt": b"oi"}))

    with pytest.raises(ConversionError, match="nenhum membro"):
        _convert(path, include=r"\.csv$")


def test_zip_member_files_do_not_outlive_the_conversion(tmp_path: Path) -> None:
    path = tmp_path / "pacote.zip"
    path.write_bytes(zip_of({"a.csv": b"x\n1\n", "b.csv": b"y\n2\n"}))

    _convert(path)

    assert sorted(p.name for p in tmp_path.iterdir()) == ["out", "pacote.zip"]
