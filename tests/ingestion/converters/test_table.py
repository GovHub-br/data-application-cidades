"""Conversão por uma função da DAG: o arquivo da raw vira uma tabela, gravada só texto.

O texto segue o `registros_para_staging_parquet` da main: os registros passam por
`DataFrame(registros)`, objeto e lista viram JSON, data vira `isoformat`, nulo
continua nulo e o resto é `str(valor)`.
"""

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.converters import Table, convert_table


def _frame(raw: Path) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mes": [pd.Timestamp("2026-08-01"), pd.Timestamp("2026-09-01")],
            "indice": [1234.5, float("nan")],
            "n": [3, 4],
            "dia": [date(2026, 8, 1), None],
            "extra": [{"a": 1}, ["x", "é"]],
            "texto": [raw.read_text(), "b"],
        }
    )


def test_the_frame_is_written_as_text_like_the_main(tmp_path: Path) -> None:
    raw = tmp_path / "incc_m.xlsx"
    raw.write_text("a")

    [parquet] = convert_table(Table(_frame), raw, tmp_path / "out")

    table = pq.read_table(parquet)
    assert parquet == tmp_path / "out" / "incc_m.parquet"
    # string ou large_string conforme o pyarrow (a main tem os dois): VARCHAR no duckdb
    assert all(
        pa.types.is_string(t) or pa.types.is_large_string(t) for t in table.schema.types
    )
    assert table.to_pylist() == [
        {
            "mes": "2026-08-01T00:00:00",
            "indice": "1234.5",
            "n": "3",
            "dia": "2026-08-01",
            "extra": '{"a": 1}',
            "texto": "a",
        },
        {
            "mes": "2026-09-01T00:00:00",
            "indice": None,
            "n": "4",
            "dia": None,
            "extra": '["x", "é"]',
            "texto": "b",
        },
    ]


def test_fallback_json_has_the_records_before_the_text(tmp_path: Path) -> None:
    raw = tmp_path / "icst.csv"
    raw.write_text("a")

    parquet, fallback = convert_table(
        Table(_frame, fallback_json=True), raw, tmp_path / "out"
    )

    assert fallback == tmp_path / "out" / "fallback_json" / "icst.json"
    records = json.loads(fallback.read_text())
    assert records[0]["mes"] == "2026-08-01 00:00:00"
    assert records[0]["indice"] == 1234.5
    assert records[1]["extra"] == ["x", "é"]
    assert parquet.name == "icst.parquet"


def test_accumulating_function_gets_the_current_staging(tmp_path: Path) -> None:
    seen: list[object] = []

    def accumulate(raw: Path, current: pd.DataFrame | None) -> pd.DataFrame:
        seen.append(None if current is None else current.to_dict("records"))
        new = pd.DataFrame({"data_pregao": ["2026-10-08"], "close": ["10"]})
        return new if current is None else pd.concat([current, new])

    raw = tmp_path / "acoes_imob.json"
    raw.write_text("{}")
    [first] = convert_table(Table(accumulate, accumulate=True), raw, tmp_path / "1")

    convert_table(Table(accumulate, accumulate=True), raw, tmp_path / "2", current=first)

    assert seen == [None, [{"data_pregao": "2026-10-08", "close": "10"}]]


def test_a_naive_datetime_keeps_its_isoformat(tmp_path: Path) -> None:
    raw = tmp_path / "x.csv"
    raw.write_text("")
    when = datetime(2026, 10, 10, 9, 30, 1, 123456)

    [parquet] = convert_table(
        Table(lambda _raw: pd.DataFrame({"dt_ingest": [when.isoformat()]})),
        raw,
        tmp_path / "out",
    )

    assert pq.read_table(parquet).to_pylist() == [
        {"dt_ingest": "2026-10-10T09:30:01.123456"}
    ]
