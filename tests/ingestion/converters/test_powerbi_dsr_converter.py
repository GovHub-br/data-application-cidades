"""Power BI (querydata, DSR): as linhas compactadas viram linhas de texto."""

import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory

MEDIDAS = ["Admitidos", "Desligados", "Saldo", "Estoque", "Variacao"]
SCHEMA = [{"N": f"M{i}", "T": 4} for i in range(5)]


def _response(rows: list[dict[str, Any]], value_dicts: Any = None) -> dict[str, Any]:
    ds: dict[str, Any] = {"N": "DS0", "PH": [{"DM0": rows}]}
    if value_dicts:
        ds["ValueDicts"] = value_dicts
    return {
        "results": [
            {
                "result": {
                    "data": {
                        "descriptor": {
                            "Select": [
                                {"Kind": 2, "Value": f"M{i}", "Name": name}
                                for i, name in enumerate(MEDIDAS)
                            ]
                        },
                        "dsr": {"Version": 2, "DS": [ds]},
                    }
                }
            }
        ]
    }


def _rows(tmp_path: Path, payload: Any) -> tuple[list[str], list[dict[str, Any]]]:
    path = tmp_path / "2026-08.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    converter = ConverterFactory.for_file(path, ConverterConfig(format="powerbi_dsr"))
    [converted] = converter.convert(path, tmp_path / "out")
    table = pq.read_table(converted.path)
    return table.column_names, table.to_pylist()


def test_published_month_uses_the_descriptor_names(tmp_path: Path) -> None:
    row = {"S": SCHEMA, "C": [85392, 80355, 5037, 1211493, "0.0041750382939783962"]}

    names, rows = _rows(tmp_path, _response([row]))

    assert names == MEDIDAS
    assert rows == [
        {
            "Admitidos": "85392",
            "Desligados": "80355",
            "Saldo": "5037",
            "Estoque": "1211493",
            "Variacao": "0.0041750382939783962",
        }
    ]


def test_null_bitmask_leaves_only_the_values_present(tmp_path: Path) -> None:
    # Mês ainda sem dado, como o Power BI devolve: Ø = 0b10111 (só M3 presente).
    row = {"S": SCHEMA, "C": [795153], "Ø": 23}

    _, rows = _rows(tmp_path, _response([row]))

    assert rows == [
        {
            "Admitidos": None,
            "Desligados": None,
            "Saldo": None,
            "Estoque": "795153",
            "Variacao": None,
        }
    ]


def test_repeat_bitmask_copies_the_previous_row(tmp_path: Path) -> None:
    rows_in = [
        {"S": SCHEMA, "C": [1, 2, 3, 4, "0.1"]},
        {"C": [9, "0.2"], "R": 0b01110},
    ]

    _, rows = _rows(tmp_path, _response(rows_in))

    assert [list(r.values()) for r in rows] == [
        ["1", "2", "3", "4", "0.1"],
        ["9", "2", "3", "4", "0.2"],
    ]


def test_value_dictionary_indexes_are_resolved(tmp_path: Path) -> None:
    schema = [{"N": "M0", "T": 1, "DN": "D0"}] + SCHEMA[1:]
    row = {"S": schema, "C": [1, 2, 3, 4, "0.5"]}

    _, rows = _rows(tmp_path, _response([row], {"D0": ["Norte", "Sul"]}))

    assert rows[0]["Admitidos"] == "Sul"


def test_query_without_rows_is_an_empty_table_with_the_measures(
    tmp_path: Path,
) -> None:
    payload = _response([])
    payload["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"] = [{}]

    names, rows = _rows(tmp_path, payload)

    assert (names, rows) == (MEDIDAS, [])


def test_response_outside_the_format_is_a_conversion_error(tmp_path: Path) -> None:
    with pytest.raises(ConversionError, match="Power BI"):
        _rows(tmp_path, {"error": {"code": "Unauthorized"}})
