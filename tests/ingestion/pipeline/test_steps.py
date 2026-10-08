"""Passos do pipeline: o que as tasks das DAGs chamam, com só caminhos entre elas."""

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.pipeline import steps
from ingestion.storage import SUCCESS_MARKER, StorageFactory
from tests.ingestion.extractors.conftest import FakeHttpServer, Route

WHEN = datetime(2026, 10, 8, 9, 0, 5, tzinfo=timezone.utc)  # 06:00:05 em Brasília


@pytest.fixture
def lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "lake"
    monkeypatch.setenv("INGESTION_STORAGE_BACKEND", "local")
    monkeypatch.setenv("INGESTION_LOCAL_ROOT", str(root))
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path / "tmp"))
    return root


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeHttpServer]:
    with FakeHttpServer() as fake:
        monkeypatch.setenv("AIRFLOW_CONN_HTTP_TEST", f"http://127.0.0.1:{fake.port}")
        yield fake


def _spec(*paths: str) -> DatasetSpec:
    return DatasetSpec(
        domain="bacen",
        dataset="financiamentos_imobiliarios",
        extractor=ExtractorConfig(
            source="api",
            conn_id="http_test",
            requests=tuple(HttpRequest(name=p.strip("/"), endpoint=p) for p in paths),
        ),
    )


def test_extract_then_convert_returns_only_prefixes(
    lake: Path, server: FakeHttpServer, tmp_path: Path
) -> None:
    server.routes["/ipca"] = Route(body=b'[{"data":"01/08/2026","valor":"-0.32"}]')
    spec = _spec("/ipca")

    raw = steps.extract_to_raw(spec, WHEN)
    latest = steps.convert_to_staging(spec, raw)

    assert raw == "raw/bacen/financiamentos_imobiliarios/2026-10-08/060005/"
    assert latest == "staging/bacen/financiamentos_imobiliarios/latest/"
    storage = StorageFactory.create("local", root=lake)
    assert storage.list(raw) == [raw + SUCCESS_MARKER, raw + "ipca.json"]
    assert storage.exists(
        "staging/bacen/financiamentos_imobiliarios/2026-10-08/060005/ipca.parquet"
    )
    assert pq.read_table(lake / latest / "ipca.parquet").to_pylist() == [
        {"data": "01/08/2026", "valor": "-0.32"}
    ]
    assert not any(p.is_file() for p in (tmp_path / "tmp").rglob("*"))


def test_extractor_function_is_resolved_inside_the_step(
    lake: Path, server: FakeHttpServer
) -> None:
    server.routes["/selic"] = Route(body=b"[]")
    resolved: list[bool] = []

    def from_variable() -> ExtractorConfig:
        resolved.append(True)
        return _spec("/selic").extractor_config()

    spec = DatasetSpec(
        domain="bacen", dataset="financiamentos_imobiliarios", extractor=from_variable
    )
    assert resolved == []

    steps.extract_to_raw(spec, WHEN)

    assert resolved == [True]


def test_missing_source_skips_the_task(lake: Path, server: FakeHttpServer) -> None:
    with pytest.raises(AirflowSkipException):
        steps.extract_to_raw(_spec("/nao-existe"), WHEN)
    assert StorageFactory.create("local", root=lake).list("raw/") == []


def test_convert_refuses_a_prefix_of_another_dataset(lake: Path) -> None:
    with pytest.raises(ValueError, match="não é uma partição"):
        steps.convert_to_staging(_spec("/x"), "raw/fgv/incc_m/2026-10-08/060005/")
