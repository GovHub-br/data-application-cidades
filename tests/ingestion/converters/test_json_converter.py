"""Conversor de json: registros em colunas texto, aninhado como texto JSON."""

import json
import tracemalloc
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory


def _rows(tmp_path: Path, text: str, **config: Any) -> list[dict[str, Any]]:
    path = tmp_path / "dados.json"
    path.write_text(text, encoding="utf-8")
    converter = ConverterFactory.for_file(path, ConverterConfig(**config))
    [converted] = converter.convert(path, tmp_path / "out")
    table = pq.read_table(converted.path)
    assert all(field.type == pa.string() for field in table.schema)
    rows: list[dict[str, Any]] = table.to_pylist()
    return rows


def test_numbers_keep_their_original_text(tmp_path: Path) -> None:
    assert _rows(tmp_path, '[{"a": 5.10, "b": 7, "c": "007", "d": -0.32}]') == [
        {"a": "5.10", "b": "7", "c": "007", "d": "-0.32"}
    ]


def test_literals_and_null(tmp_path: Path) -> None:
    assert _rows(tmp_path, '[{"ok": true, "nao": false, "nada": null}]') == [
        {"ok": "true", "nao": "false", "nada": None}
    ]


def test_nested_values_become_json_text(tmp_path: Path) -> None:
    [row] = _rows(tmp_path, '[{"id": 1, "local": {"uf": "DF", "ids": [1, 2.5]}}]')

    assert json.loads(row["local"]) == {"uf": "DF", "ids": [1, 2.5]}
    assert row["local"] == '{"uf": "DF", "ids": [1, 2.5]}'


def test_columns_are_the_union_of_keys_in_order_of_appearance(tmp_path: Path) -> None:
    rows = _rows(tmp_path, '[{"a": 1, "b": 2}, {"c": 3, "a": 4}]')

    assert rows == [{"a": "1", "b": "2", "c": None}, {"a": "4", "b": None, "c": "3"}]


def test_record_path_reaches_nested_lists(tmp_path: Path) -> None:
    text = '{"meta": {"n": 2}, "resultados": {"series": [{"v": "1"}, {"v": "2"}]}}'

    assert _rows(tmp_path, text, record_path="resultados.series.item") == [
        {"v": "1"},
        {"v": "2"},
    ]


def test_records_that_are_not_objects_go_to_a_value_column(tmp_path: Path) -> None:
    assert _rows(tmp_path, '["a", 2, null]') == [
        {"value": "a"},
        {"value": "2"},
        {"value": None},
    ]


def test_no_records_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="nenhum registro"):
        _rows(tmp_path, "[]")


def test_invalid_json_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError):
        _rows(tmp_path, '[{"a": 1},')


def _peak_converting(tmp_path: Path, count: int) -> int:
    path = tmp_path / f"grande_{count}.json"
    record = '{"data":"01/08/2026","valor":"123456.789","serie":"20704","obs":"x"}'
    with path.open("w") as target:
        target.write("[" + ",".join([record] * count) + "]")
    converter = ConverterFactory.for_file(path, ConverterConfig())
    tracemalloc.start()
    try:
        [converted] = converter.convert(path, tmp_path / f"out_{count}")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert converted.rows == count
    return peak


def test_memory_does_not_grow_with_the_number_of_records(tmp_path: Path) -> None:
    # 4x mais registros, mesmo pico: a memória é a de um lote, não a do arquivo.
    small = _peak_converting(tmp_path, 30_000)
    large = _peak_converting(tmp_path, 120_000)

    assert large < small * 1.5, f"{small / 2**20:.1f} MiB -> {large / 2**20:.1f} MiB"
