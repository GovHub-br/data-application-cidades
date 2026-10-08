"""Casos de borda do texto delimitado (guia §3.3) no conversor de csv."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory


def _rows(
    tmp_path: Path, data: bytes, name: str = "f.csv", **config: Any
) -> list[dict[str, Any]]:
    path = tmp_path / name
    path.write_bytes(data)
    converter = ConverterFactory.for_file(path, ConverterConfig(**config))
    [converted] = converter.convert(path, tmp_path / "out")
    table = pq.read_table(converted.path)
    assert all(field.type == pa.string() for field in table.schema)
    rows: list[dict[str, Any]] = table.to_pylist()
    return rows


def test_null_is_not_the_same_as_empty_string(tmp_path: Path) -> None:
    assert _rows(tmp_path, b'a,b,c\n,"",x\n') == [{"a": None, "b": "", "c": "x"}]


def test_bom_is_not_part_of_the_first_column_name(tmp_path: Path) -> None:
    assert _rows(tmp_path, "﻿mes,valor\n1,2\n".encode("utf-8")) == [
        {"mes": "1", "valor": "2"}
    ]


def test_values_are_never_typed(tmp_path: Path) -> None:
    assert _rows(tmp_path, b"cod,valor,data\n007,1.50,2026-10-08\n") == [
        {"cod": "007", "valor": "1.50", "data": "2026-10-08"}
    ]


def test_column_that_is_all_null_is_still_string(tmp_path: Path) -> None:
    assert _rows(tmp_path, b"a,b\n1,\n2,\n") == [
        {"a": "1", "b": None},
        {"a": "2", "b": None},
    ]


def test_empty_and_repeated_header_names(tmp_path: Path) -> None:
    rows = _rows(tmp_path, b'valor,,valor,""\n1,2,3,4\n')

    assert list(rows[0]) == ["valor", "column_2", "valor_2", "column_4"]


def test_latin1_with_semicolon(tmp_path: Path) -> None:
    data = "município;valor\nSão Paulo;1,5\n".encode("latin-1")

    assert _rows(tmp_path, data, encoding="latin-1", delimiter=";") == [
        {"município": "São Paulo", "valor": "1,5"}
    ]


def test_utf16_tsv_with_preamble_like_the_tesouro_report(tmp_path: Path) -> None:
    preamble = "".join(f"Relatório Tesouro Gerencial, linha {n}\n" for n in range(12))
    data = (preamble + "Unidade\tDotação\n53000\t1.234,56\n").encode("utf-16")

    rows = _rows(tmp_path, data, encoding="utf-16", delimiter="\t", skip_rows=12)

    assert rows == [{"Unidade": "53000", "Dotação": "1.234,56"}]


def test_quoted_newline_stays_inside_the_value(tmp_path: Path) -> None:
    assert _rows(tmp_path, b'a,b\n"linha 1\nlinha 2",x\n') == [
        {"a": "linha 1\nlinha 2", "b": "x"}
    ]


def test_header_only_file_gives_empty_parquet_with_schema(tmp_path: Path) -> None:
    path = tmp_path / "vazio.csv"
    path.write_bytes(b"a,b\n")
    [converted] = ConverterFactory.for_file(path, ConverterConfig()).convert(
        path, tmp_path / "out"
    )

    table = pq.read_table(converted.path)
    assert (table.num_rows, table.schema.names) == (0, ["a", "b"])


def test_ragged_row_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError):
        _rows(tmp_path, b"a,b\n1,2,3\n")


def test_empty_file_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="vazio"):
        _rows(tmp_path, b"")


def test_large_file_converts_with_bounded_arrow_memory(tmp_path: Path) -> None:
    # ~128 MiB de csv; o pool do leitor não pode crescer na proporção do arquivo.
    path = tmp_path / "grande.csv"
    line = b"0123456789," * 9 + b"0123456789\n"  # 110 bytes, 10 colunas
    with path.open("wb") as target:
        target.write(b",".join(f"c{i}".encode() for i in range(10)) + b"\n")
        block = line * 10_000
        for _ in range(122):
            target.write(block)
    pool = pa.proxy_memory_pool(pa.default_memory_pool())
    converter = ConverterFactory.for_file(path, ConverterConfig())
    converter.memory_pool = pool

    [converted] = converter.convert(path, tmp_path / "out")

    assert converted.rows == 1_220_000
    assert pool.max_memory() < 64 * 1024 * 1024, f"{pool.max_memory() / 2**20:.0f} MiB"
