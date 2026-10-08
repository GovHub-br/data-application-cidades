"""Cabeçalho: só o mínimo para virar schema; renome de verdade é da prata."""

import pytest

from ingestion.converters import fix_header


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (["mês", "Valor (R$)", "  x "], ["mês", "Valor (R$)", "  x "]),
        (["a", "", "b", None], ["a", "column_2", "b", "column_4"]),
        (["valor", "valor", "valor"], ["valor", "valor_2", "valor_3"]),
        (["a", "a_2", "a"], ["a", "a_2", "a_3"]),
        (["", "column_1"], ["column_1", "column_1_2"]),
    ],
)
def test_fix_header_only_fills_empty_and_dedupes(
    header: list[str | None], expected: list[str]
) -> None:
    assert fix_header(header) == expected
