"""Conversor de json: registros em colunas texto, aninhado como texto JSON."""

import json
import tracemalloc
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.converters import (
    ConversionError,
    ConverterConfig,
    ConverterFactory,
    Field,
)


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


def test_key_column_turns_the_keys_of_an_object_into_rows(tmp_path: Path) -> None:
    # Alpha Vantage: a data do pregão é chave, não campo.
    text = (
        '{"Meta Data": {"2. Symbol": "IMOB"}, "Time Series (Daily)": {'
        '"2026-10-08": {"1. open": "1.10", "5. volume": "7"},'
        '"2026-10-07": {"1. open": "1.05", "5. volume": "9"}}}'
    )

    rows = _rows(
        tmp_path, text, record_path="Time Series (Daily)", key_column="data_pregao"
    )

    assert rows == [
        {"data_pregao": "2026-10-08", "1. open": "1.10", "5. volume": "7"},
        {"data_pregao": "2026-10-07", "1. open": "1.05", "5. volume": "9"},
    ]


def test_key_column_with_scalar_values_uses_the_value_column(tmp_path: Path) -> None:
    rows = _rows(
        tmp_path,
        '{"serie": {"202401": "1.2", "202402": "..."}}',
        record_path="serie",
        key_column="periodo",
    )

    assert rows == [
        {"periodo": "202401", "value": "1.2"},
        {"periodo": "202402", "value": "..."},
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


NESTED = {
    "itens": [
        {
            "id": 1,
            "nome": "A",
            "grupos": [
                {
                    "tags": [{"id": "t1", "rotulo": {"10": "dez"}}],
                    "pontos": [
                        {"local": {"id": "BR"}, "serie": {"2024": "1.5", "2025": "..."}}
                    ],
                },
                {
                    "tags": [],
                    "pontos": [{"local": {"id": "SP"}, "serie": {"2025": "7"}}],
                },
            ],
        }
    ]
}


def test_nested_lists_and_exploded_keys_with_declared_columns(tmp_path: Path) -> None:
    rows = _rows(
        tmp_path,
        json.dumps(NESTED),
        record_path="itens.item",
        nested=("grupos", "pontos"),
        explode_keys="serie",
        columns={
            "id": Field("item.id"),
            "local": Field("pontos.local.id"),
            "tag": Field("grupos.tags[*].id", join="|", default="0"),
            "rotulo_id": Field("grupos.tags[*].rotulo{keys}", join="|", default="0"),
            "rotulo": Field("grupos.tags[*].rotulo{values}", join=" | ", default=""),
            "ano": Field("key"),
            "valor": Field("value"),
        },
    )

    assert rows == [
        {
            "id": "1",
            "local": "BR",
            "tag": "t1",
            "rotulo_id": "10",
            "rotulo": "dez",
            "ano": "2024",
            "valor": "1.5",
        },
        {
            "id": "1",
            "local": "BR",
            "tag": "t1",
            "rotulo_id": "10",
            "rotulo": "dez",
            "ano": "2025",
            "valor": "...",
        },
        {
            "id": "1",
            "local": "SP",
            "tag": "0",
            "rotulo_id": "0",
            "rotulo": "",
            "ano": "2025",
            "valor": "7",
        },
    ]


def test_field_with_many_values_and_no_join_is_an_error(tmp_path: Path) -> None:
    text = '[{"xs": [{"v": 1}, {"v": 2}]}]'

    with pytest.raises(ConversionError, match="join"):
        _rows(tmp_path, text, columns={"v": Field("item.xs[*].v")})


def test_field_missing_uses_the_default_or_null(tmp_path: Path) -> None:
    rows = _rows(
        tmp_path,
        '[{"a": 1}]',
        columns={
            "a": Field("item.a"),
            "b": Field("item.b"),
            "c": Field("item.c", default="x"),
        },
    )

    assert rows == [{"a": "1", "b": None, "c": "x"}]


def test_nested_without_columns_flattens_the_last_level(tmp_path: Path) -> None:
    rows = _rows(
        tmp_path,
        json.dumps(NESTED),
        record_path="itens.item",
        nested=("grupos", "pontos"),
    )

    assert rows[0] == {"local": '{"id": "BR"}', "serie": '{"2024": "1.5", "2025": "..."}'}
    assert len(rows) == 2
