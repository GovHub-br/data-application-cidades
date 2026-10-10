"""Layout do lake, o da main: a raw e a staging espelham o caminho da fonte."""

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


@pytest.mark.parametrize(
    ("domain", "name", "expected"),
    [
        ("fgv", "incc_m.xlsx", "raw/fgv/incc_m.xlsx"),
        (
            "sftp",
            "fabrica/GEFUS/ANTERIORES/X 2023.TXT",
            "raw/sftp/fabrica/GEFUS/ANTERIORES/X 2023.TXT",
        ),
        (
            "abecip",
            "fallback_json/poupanca.json",
            "raw/abecip/fallback_json/poupanca.json",
        ),
    ],
)
def test_raw_key_is_domain_and_the_path_of_the_source(
    domain: str, name: str, expected: str
) -> None:
    assert layout.raw_key(domain, name) == expected


@pytest.mark.parametrize(
    "name", ["", "/abs.csv", "a//b.csv", "../fora.csv", "a/./b.csv", "pasta/"]
)
def test_raw_key_rejects_paths_that_escape_or_are_empty(name: str) -> None:
    with pytest.raises(ValueError):
        layout.raw_key("sftp", name)


@pytest.mark.parametrize(
    ("raw_key", "suffix", "expected"),
    [
        ("raw/fgv/incc_m.xlsx", None, "staging/fgv/incc_m.parquet"),
        (
            "raw/sftp/fabrica/GEFUS/X.TXT",
            None,
            "staging/sftp/fabrica/GEFUS/X.parquet",
        ),
        (
            "raw/abecip/financiamentos_modalidade.xlsx",
            "bd_unidades",
            "staging/abecip/financiamentos_modalidade__bd_unidades.parquet",
        ),
        ("raw/sftp/fabrica/a.b.c.csv", None, "staging/sftp/fabrica/a.b.c.parquet"),
    ],
)
def test_staging_key_mirrors_the_raw_without_the_extension(
    raw_key: str, suffix: str | None, expected: str
) -> None:
    assert layout.staging_key(raw_key, suffix) == expected


def test_staging_key_refuses_a_key_outside_the_raw() -> None:
    with pytest.raises(ValueError):
        layout.staging_key("staging/fgv/incc_m.parquet")
