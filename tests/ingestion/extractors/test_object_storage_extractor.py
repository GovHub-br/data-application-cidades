"""Estratégia `object_storage`: objetos que outro processo já gravou no bucket."""

from pathlib import Path

import pytest

from ingestion.extractors import (
    ExtractionError,
    ExtractorConfig,
    ExtractorFactory,
    ObjectQuery,
)
from ingestion.storage import StorageFactory
from tests.ingestion.extractors.conftest import WHEN

QUERY = ObjectQuery(
    prefix="raw/abecip/",
    pattern=r"raw/abecip/(\d{4}-\d{2})/financiamentos_por_instituicao\.json",
    rename=r"\1.json",
)


@pytest.fixture
def bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "bucket"
    monkeypatch.setenv("INGESTION_STORAGE_BACKEND", "local")
    monkeypatch.setenv("INGESTION_LOCAL_ROOT", str(root))
    # A fonte é o bucket real: o prefixo de teste vale para onde gravamos, não
    # para onde o outro processo grava.
    monkeypatch.setenv("INGESTION_STORAGE_PREFIX", "tests/")
    storage = StorageFactory.create("local", root=root)
    for key, body in {
        "raw/abecip/2026-06/financiamentos_por_instituicao.json": b"[1]",
        "raw/abecip/2026-07/financiamentos_por_instituicao.json": b"[2]",
        "raw/abecip/2026-07/recursos_livres.json": b"[3]",
        # a nossa própria raw do dataset não casa com o padrão
        "raw/abecip/financiamentos_por_instituicao/2026-10-09/060000/x.json": b"[4]",
    }.items():
        local = tmp_path / "src" / key
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(body)
        storage.put_file(key, local)
    return root


def _extract(tmp_path: Path, query: ObjectQuery) -> dict[str, bytes]:
    config = ExtractorConfig(source="object_storage", objects=query)
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    return {
        part.name: part.path.read_bytes() for part in extractor.extract(tmp_path / "work")
    }


def test_copies_only_the_objects_that_match_with_the_new_name(
    bucket: Path, tmp_path: Path
) -> None:
    assert _extract(tmp_path, QUERY) == {"2026-06.json": b"[1]", "2026-07.json": b"[2]"}


def test_nothing_matching_is_an_extraction_error(bucket: Path, tmp_path: Path) -> None:
    query = ObjectQuery(prefix="raw/abecip/", pattern=r"raw/abecip/nada\.json")

    with pytest.raises(ExtractionError, match="nenhum objeto"):
        _extract(tmp_path, query)


def test_config_without_query_is_rejected() -> None:
    with pytest.raises(ValueError, match="objects"):
        ExtractorFactory.create(
            ExtractorConfig(source="object_storage"), ingestion_time=WHEN
        )
