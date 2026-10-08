"""Layout do lake: segmentos seguros, partição por data de ingestão e prefixos."""

import pytest

from ingestion import layout


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("incc_m", "incc_m"),
        ("ibge", "ibge"),
        ("sinapi-2026.v1", "sinapi-2026.v1"),
        ("manual__2026-10-08T12:00:00+00:00", "manual__2026-10-08T12_00_00_00_00"),
        ("pasta/arquivo", "pasta_arquivo"),
        ("caça níquel", "ca_a_n_quel"),
    ],
)
def test_safe_segment_keeps_safe_chars_and_replaces_the_rest(
    value: str, expected: str
) -> None:
    assert layout.safe_segment(value) == expected


@pytest.mark.parametrize("value", ["", ".", ".."])
def test_safe_segment_rejects_empty_and_relative_segments(value: str) -> None:
    with pytest.raises(ValueError):
        layout.safe_segment(value)
