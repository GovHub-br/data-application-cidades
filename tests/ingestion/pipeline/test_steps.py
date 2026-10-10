"""Passos do pipeline: o que as tasks das DAGs chamam, com só chaves entre elas."""

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from ingestion.converters import ConverterConfig, Table
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.pipeline import NothingNew, steps
from ingestion.storage import StorageFactory
from tests.ingestion.converters.conftest import xlsx_of
from tests.ingestion.extractors.conftest import FakeHttpServer, Route

WHEN = datetime(2026, 10, 8, 9, 0, 5, tzinfo=timezone.utc)


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


def _spec(*paths: str, converter: ConverterConfig | Table | None = None) -> DatasetSpec:
    return DatasetSpec(
        domain="bacen",
        dataset="financiamentos_imobiliarios",
        extractor=ExtractorConfig(
            source="api",
            conn_id="http_test",
            requests=tuple(HttpRequest(name=p.strip("/"), endpoint=p) for p in paths),
        ),
        converter=converter or ConverterConfig(),
    )


def test_extract_then_convert_returns_only_keys(
    lake: Path, server: FakeHttpServer, tmp_path: Path
) -> None:
    server.routes["/ipca"] = Route(body=b'[{"data":"01/08/2026","valor":"-0.32"}]')
    spec = _spec("/ipca")

    landed = steps.extract_to_raw(spec, WHEN)
    staged = steps.convert_to_staging(spec, landed)

    assert landed == {"financiamentos_imobiliarios": ["raw/bacen/ipca.json"]}
    assert staged == {"financiamentos_imobiliarios": ["staging/bacen/ipca.parquet"]}
    storage = StorageFactory.create("local", root=lake)
    assert storage.list("") == ["raw/bacen/ipca.json", "staging/bacen/ipca.parquet"]
    assert pq.read_table(lake / "staging/bacen/ipca.parquet").to_pylist() == [
        {"data": "01/08/2026", "valor": "-0.32"}
    ]
    assert not any(p.is_file() for p in (tmp_path / "tmp").rglob("*"))


def test_a_workbook_gives_one_parquet_per_sheet(lake: Path, tmp_path: Path) -> None:
    workbook = tmp_path / "planilha.xlsx"
    workbook.write_bytes(xlsx_of({"BD Unidades": [["a"], ["1"]], "SBPE": [["b"], ["2"]]}))
    StorageFactory.create("local", root=lake).put_file(
        "raw/bacen/planilha.xlsx", workbook
    )

    staged = steps.convert_to_staging(
        _spec("/x"), {"financiamentos_imobiliarios": ["raw/bacen/planilha.xlsx"]}
    )

    assert staged == {
        "financiamentos_imobiliarios": [
            "staging/bacen/planilha__BD_Unidades.parquet",
            "staging/bacen/planilha__SBPE.parquet",
        ]
    }


def test_extractor_function_is_resolved_inside_the_step(
    lake: Path, server: FakeHttpServer
) -> None:
    server.routes["/selic"] = Route(body=b"[]")
    resolved: list[bool] = []

    def from_variable(entrada: object) -> ExtractorConfig:
        resolved.append(True)
        return _spec("/selic").extractor_config()

    spec = DatasetSpec(
        domain="bacen", dataset="financiamentos_imobiliarios", extractor=from_variable
    )
    assert resolved == []

    steps.extract_to_raw(spec, WHEN)

    assert resolved == [True]


def test_missing_source_is_nothing_new(lake: Path, server: FakeHttpServer) -> None:
    # A DAG traduz o NothingNew em skip; os passos não conhecem o Airflow.
    with pytest.raises(NothingNew):
        steps.extract_to_raw(_spec("/nao-existe"), WHEN)
    assert StorageFactory.create("local", root=lake).list("raw/") == []


def test_convert_refuses_a_key_of_another_domain(lake: Path) -> None:
    with pytest.raises(ValueError, match="não é da raw de bacen"):
        steps.convert_to_staging(
            _spec("/x"), {"financiamentos_imobiliarios": ["raw/fgv/incc_m.xlsx"]}
        )


def _incc(raw: Path) -> pd.DataFrame:
    frame = pd.read_json(raw)
    return frame.assign(dt_ingest="2026-10-10T03:00:00")


def test_a_table_function_gives_the_staging_and_the_fallback(
    lake: Path, server: FakeHttpServer
) -> None:
    server.routes["/incc_m"] = Route(body=b'[{"mes":"jan/26","indice":1234.5}]')
    spec = _spec("/incc_m", converter=Table(_incc, fallback_json=True))

    staged = steps.convert_to_staging(spec, steps.extract_to_raw(spec, WHEN))

    assert staged == {"financiamentos_imobiliarios": ["staging/bacen/incc_m.parquet"]}
    assert pq.read_table(lake / "staging/bacen/incc_m.parquet").to_pylist() == [
        {"mes": "jan/26", "indice": "1234.5", "dt_ingest": "2026-10-10T03:00:00"}
    ]
    assert json.loads((lake / "raw/bacen/fallback_json/incc_m.json").read_text()) == [
        {"mes": "jan/26", "indice": 1234.5, "dt_ingest": "2026-10-10T03:00:00"}
    ]


def test_an_accumulating_table_gets_the_staging_it_wrote_before(
    lake: Path, server: FakeHttpServer
) -> None:
    def accumulate(raw: Path, current: pd.DataFrame | None) -> pd.DataFrame:
        new = pd.DataFrame(json.loads(raw.read_text()))
        if current is None:
            return new
        merged = pd.concat([current, new]).drop_duplicates("dia", keep="last")
        return merged.reset_index(drop=True)

    route = server.routes["/imob"] = Route(body=b'[{"dia":"1","v":"a"}]')
    spec = _spec("/imob", converter=Table(accumulate, accumulate=True))
    steps.convert_to_staging(spec, steps.extract_to_raw(spec, WHEN))
    route.body = b'[{"dia":"1","v":"b"},{"dia":"2","v":"c"}]'

    steps.convert_to_staging(spec, steps.extract_to_raw(spec, WHEN))

    assert pq.read_table(lake / "staging/bacen/imob.parquet").to_pylist() == [
        {"dia": "1", "v": "b"},
        {"dia": "2", "v": "c"},
    ]
