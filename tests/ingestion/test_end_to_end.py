"""Da fonte ao `select * from read_parquet(...)` que o pg_duckdb faz, com as peças reais.

Extrator -> raw/<fonte>/<arquivo> -> staging/<fonte>/<arquivo>.parquet -> DuckDB
lendo o Parquet com a mesma sintaxe que o pg_duckdb executa dentro do banco.
"""

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.pipeline import steps
from tests.ingestion.extractors.conftest import FakeHttpServer, Route

FIRST = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
SECOND = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)

SGS = DatasetSpec(
    domain="bacen",
    dataset="sgs",
    extractor=ExtractorConfig(
        source="api",
        conn_id="http_test",
        requests=(
            HttpRequest(name="ipca", endpoint="/dados/serie/bcdata.sgs.433/dados"),
            HttpRequest(name="selic_meta", endpoint="/dados/serie/bcdata.sgs.432/dados"),
        ),
    ),
)


@pytest.fixture
def lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "lake"
    monkeypatch.setenv("INGESTION_STORAGE_BACKEND", "local")
    monkeypatch.setenv("INGESTION_LOCAL_ROOT", str(root))
    monkeypatch.setenv("LAKE_TMPDIR", str(tmp_path / "tmp"))
    return root


@pytest.fixture
def bacen_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeHttpServer]:
    with FakeHttpServer() as server:
        monkeypatch.setenv("AIRFLOW_CONN_HTTP_TEST", f"http://127.0.0.1:{server.port}")
        yield server


def _ingest(when: datetime) -> dict[str, list[str]]:
    return steps.convert_to_staging(SGS, steps.extract_to_raw(SGS, when))


def test_bacen_series_reach_the_pg_duckdb_select_as_text(
    lake: Path, bacen_server: FakeHttpServer
) -> None:
    bacen_server.routes["/dados/serie/bcdata.sgs.433/dados"] = Route(
        body=b'[{"data":"01/07/2026","valor":"0.26"},{"data":"01/08/2026","valor":"-0.32"}]'
    )
    bacen_server.routes["/dados/serie/bcdata.sgs.432/dados"] = Route(
        body=b'[{"data":"08/10/2026","valor":"13.75"}]'
    )

    assert _ingest(FIRST) == {
        "sgs": ["staging/bacen/ipca.parquet", "staging/bacen/selic_meta.parquet"]
    }
    ipca = duckdb.sql(f"select * from read_parquet('{lake}/staging/bacen/ipca.parquet')")

    assert ipca.columns == ["data", "valor"]
    assert ipca.types == ["VARCHAR", "VARCHAR"]
    assert ipca.fetchall() == [("01/07/2026", "0.26"), ("01/08/2026", "-0.32")]


def test_each_ingestion_overwrites_the_raw_and_the_staging(
    lake: Path, bacen_server: FakeHttpServer
) -> None:
    route = bacen_server.routes["/dados/serie/bcdata.sgs.433/dados"] = Route(
        body=b'[{"data":"01/08/2026","valor":"-0.32"}]'
    )
    bacen_server.routes["/dados/serie/bcdata.sgs.432/dados"] = Route(body=b'[{"v":"1"}]')
    _ingest(FIRST)

    # Revisão da fonte: o mesmo mês com outro valor, e um mês novo.
    route.body = (
        b'[{"data":"01/08/2026","valor":"-0.30"},{"data":"01/09/2026","valor":"0.48"}]'
    )
    _ingest(SECOND)

    assert (lake / "raw/bacen/ipca.json").read_bytes() == route.body
    ipca = duckdb.sql(f"select * from read_parquet('{lake}/staging/bacen/ipca.parquet')")
    assert ipca.fetchall() == [("01/08/2026", "-0.30"), ("01/09/2026", "0.48")]
    assert sorted(p.name for p in (lake / "staging/bacen").iterdir()) == [
        "ipca.parquet",
        "selic_meta.parquet",
    ]
