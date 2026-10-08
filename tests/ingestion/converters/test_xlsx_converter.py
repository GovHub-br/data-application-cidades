"""Conversor de xlsx: cada aba em Parquet texto, com o valor da célula como texto."""

from datetime import datetime, time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from openpyxl import Workbook

from ingestion.converters import ConversionError, ConverterConfig, ConverterFactory


def _workbook(tmp_path: Path, sheets: dict[str, list[list[Any]]]) -> Path:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for title, rows in sheets.items():
        sheet = workbook.create_sheet(title)
        for row in rows:
            sheet.append(row)
    path = tmp_path / "planilha.xlsx"
    workbook.save(path)
    return path


def _convert(path: Path, **config: Any) -> dict[str, list[dict[str, Any]]]:
    converter = ConverterFactory.for_file(path, ConverterConfig(**config))
    tables = {}
    for converted in converter.convert(path, path.parent / "out"):
        table = pq.read_table(converted.path)
        assert all(field.type == pa.string() for field in table.schema)
        tables[converted.name] = table.to_pylist()
    return tables


def test_cell_values_become_text(tmp_path: Path) -> None:
    path = _workbook(
        tmp_path,
        {
            "s": [
                ["int", "float", "repr", "data", "hora", "bool", "texto", "vazio"],
                [
                    7,
                    1.5,
                    1234567.891,
                    datetime(2026, 10, 8),
                    time(9, 30),
                    True,
                    "007",
                    None,
                ],
            ]
        },
    )

    assert _convert(path) == {
        "planilha.parquet": [
            {
                "int": "7",
                "float": "1.5",
                "repr": "1234567.891",
                "data": "2026-10-08T00:00:00",
                "hora": "09:30:00",
                "bool": "true",
                "texto": "007",
                "vazio": None,
            }
        ]
    }


def test_header_row_skips_title_lines(tmp_path: Path) -> None:
    path = _workbook(
        tmp_path,
        {"s": [["Série histórica do INCC-M"], [], ["mes", "indice"], ["jan/26", 10]]},
    )

    assert _convert(path, header_row=3) == {
        "planilha.parquet": [{"mes": "jan/26", "indice": "10"}]
    }


def test_each_sheet_is_a_file_with_suffix_and_empty_sheets_are_skipped(
    tmp_path: Path,
) -> None:
    path = _workbook(
        tmp_path,
        {"Poupança": [["a"], [1]], "vazia": [], "SBPE": [["b"], [2]]},
    )

    assert _convert(path) == {
        "planilha__Poupan_a.parquet": [{"a": "1"}],
        "planilha__SBPE.parquet": [{"b": "2"}],
    }


def test_chosen_sheet_has_no_suffix(tmp_path: Path) -> None:
    path = _workbook(tmp_path, {"A": [["a"], [1]], "B": [["b"], [2]]})

    assert _convert(path, sheet="B") == {"planilha.parquet": [{"b": "2"}]}


def test_include_filters_sheets_by_regex(tmp_path: Path) -> None:
    path = _workbook(
        tmp_path, {"2025": [["a"], [1]], "2026": [["a"], [2]], "notas": [["x"]]}
    )

    assert list(_convert(path, include=r"^\d{4}$")) == [
        "planilha__2025.parquet",
        "planilha__2026.parquet",
    ]


def test_missing_sheet_lists_the_existing_ones(tmp_path: Path) -> None:
    path = _workbook(tmp_path, {"A": [["a"]]})

    with pytest.raises(ConversionError, match="'A'"):
        _convert(path, sheet="Z")


def test_trailing_empty_columns_are_dropped_but_unnamed_data_is_kept(
    tmp_path: Path,
) -> None:
    # Coluna sem nome com dado fica (column_3); colunas de enfeite vazias somem.
    rows: list[list[Any]] = [
        ["a", "b", None, None, None],
        [1, 2, "nota", None, None],
        [3, 4, None],
    ]
    path = _workbook(tmp_path, {"s": rows})

    assert _convert(path) == {
        "planilha.parquet": [
            {"a": "1", "b": "2", "column_3": "nota"},
            {"a": "3", "b": "4", "column_3": None},
        ]
    }
