"""Layout do lake: segmentos seguros, partição por data de ingestão e prefixos."""

from datetime import datetime, timedelta, timezone

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
    ("run_after", "expected"),
    [
        # Meio do dia: 15h UTC são 12h em Brasília.
        (datetime(2026, 10, 8, 15, 0, 5, tzinfo=timezone.utc), "2026-10-08/120005"),
        # Virada: em UTC já é dia 9, em Brasília ainda é dia 8.
        (datetime(2026, 10, 9, 2, 30, tzinfo=timezone.utc), "2026-10-08/233000"),
        # Já em outro fuso: converte do mesmo jeito.
        (
            datetime(2026, 10, 8, 9, 0, tzinfo=timezone(timedelta(hours=-3))),
            "2026-10-08/090000",
        ),
    ],
)
def test_ingestion_partition_uses_brasilia_date_and_time(
    run_after: datetime, expected: str
) -> None:
    assert layout.ingestion_partition(run_after) == expected


def test_ingestion_partition_rejects_naive_datetime() -> None:
    # Sem fuso não dá para saber o dia em Brasília; adivinhar trocaria a pasta do dia.
    with pytest.raises(ValueError):
        layout.ingestion_partition(datetime(2026, 10, 8, 12, 0))


def test_raw_prefix_is_domain_dataset_date_time() -> None:
    assert (
        layout.raw_prefix("ibge", "sinapi", "2026-10-08/060000")
        == "raw/ibge/sinapi/2026-10-08/060000/"
    )


def test_staging_prefix_mirrors_raw() -> None:
    assert (
        layout.staging_prefix("ibge", "sinapi", "2026-10-08/060000")
        == "staging/ibge/sinapi/2026-10-08/060000/"
    )


def test_prefix_sanitizes_domain_and_dataset() -> None:
    assert (
        layout.raw_prefix("siafi tesouro", "dotação", "2026-10-08/060000")
        == "raw/siafi_tesouro/dota__o/2026-10-08/060000/"
    )


@pytest.mark.parametrize(
    "partition",
    ["2026-10-08", "2026-10-08/0600", "08-10-2026/060000", "2026-10-08/060000/x", ""],
)
def test_prefix_rejects_malformed_partition(partition: str) -> None:
    with pytest.raises(ValueError):
        layout.raw_prefix("ibge", "sinapi", partition)


def test_latest_prefix_is_the_published_folder_of_the_dataset() -> None:
    assert layout.latest_prefix("siafi tesouro", "dotação") == (
        "staging/siafi_tesouro/dota__o/latest/"
    )
