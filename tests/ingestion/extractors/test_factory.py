"""Fábrica, configuração e erros dos extratores."""

import dataclasses
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.extractors import (
    ExtractionError,
    Extractor,
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
    MailQuery,
    RawFile,
    SourceNotFoundError,
)

WHEN = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)


@pytest.fixture
def dummy_source() -> Iterator[str]:
    @ExtractorFactory.register("dummy")
    class DummyExtractor(Extractor):
        def extract(self, work_dir: Path) -> Iterator[RawFile]:
            return iter(())

    yield "dummy"
    ExtractorFactory._registry.pop("dummy")


def test_factory_builds_the_registered_strategy(dummy_source: str) -> None:
    config = ExtractorConfig(source=dummy_source, conn_id="http_x")

    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)

    assert type(extractor).__name__ == "DummyExtractor"
    assert extractor.config is config
    assert extractor.ingestion_time == WHEN


def test_factory_lists_registered_sources_on_unknown_name(dummy_source: str) -> None:
    with pytest.raises(ValueError, match="dummy"):
        ExtractorFactory.create(
            ExtractorConfig(source="ftp", conn_id="x"), ingestion_time=WHEN
        )


def test_factory_rejects_naive_ingestion_time(dummy_source: str) -> None:
    config = ExtractorConfig(source=dummy_source, conn_id="x")

    with pytest.raises(ValueError):
        ExtractorFactory.create(config, ingestion_time=datetime(2026, 10, 8, 9, 0))


def test_factory_builds_a_new_instance_per_call(dummy_source: str) -> None:
    config = ExtractorConfig(source=dummy_source, conn_id="x")

    first = ExtractorFactory.create(config, ingestion_time=WHEN)
    second = ExtractorFactory.create(config, ingestion_time=WHEN)

    assert first is not second


def test_config_is_frozen_and_carries_only_the_connection_name() -> None:
    config = ExtractorConfig(
        source="api",
        conn_id="http_bacen",
        requests=(HttpRequest(name="20704", endpoint="/bcdata.sgs.20704/dados"),),
    )

    with pytest.raises(dataclasses.FrozenInstanceError):
        config.conn_id = "outra"  # type: ignore[misc]
    assert config.requests[0].method == "GET"
    assert config.mail is None


def test_mail_query_defaults() -> None:
    query = MailQuery(sender="tesouro@exemplo.gov.br", subject="dotacao")

    assert query.folder == "INBOX"
    assert query.attachment_pattern == r".*"


def test_source_not_found_is_an_extraction_error() -> None:
    assert issubclass(SourceNotFoundError, ExtractionError)
