"""Modos de carga e as regras que valem igual no Python e no fonte_lake do dbt."""

import pytest

from ingestion.loaders import LoadMode, LoadResult, validate_load


def test_modes_are_the_three_of_the_guide() -> None:
    assert [mode.value for mode in LoadMode] == ["overwrite", "merge", "append"]
    assert LoadMode("merge") is LoadMode.MERGE


def test_overwrite_and_append_take_no_keys() -> None:
    assert validate_load(LoadMode.OVERWRITE, ()) == ()
    assert validate_load("append", []) == ()


def test_merge_returns_its_keys() -> None:
    assert validate_load("merge", ["serie", "data"]) == ("serie", "data")


@pytest.mark.parametrize(
    ("mode", "keys", "message"),
    [
        ("merge", (), "merge exige keys"),
        ("overwrite", ("data",), "keys só no merge"),
        ("append", ("data",), "keys só no merge"),
        ("merge", ("data", "data"), "repetida"),
        ("upsert", (), "upsert"),
    ],
)
def test_invalid_combinations_are_rejected(
    mode: str, keys: tuple[str, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_load(mode, keys)


def test_load_result_is_a_plain_record() -> None:
    result = LoadResult(mode=LoadMode.MERGE, table="bronze.bronze_bacen_sgs", rows=42)

    assert (result.mode, result.table, result.rows) == (
        "merge",
        "bronze.bronze_bacen_sgs",
        42,
    )
