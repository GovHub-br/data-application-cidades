"""Da fonte ao `select * from read_parquet(...)` do bronze, com as peças reais.

Extrator -> land na raw -> convert_partition -> latest/ -> DuckDB lendo o Parquet
com a mesma sintaxe que o pg_duckdb executa nos modelos bronze do dbt.
"""

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from ingestion.converters import ConverterConfig, convert_partition
from ingestion.extractors import (
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
    write_stream,
)
from ingestion.layout import ingestion_partition, raw_prefix, staging_prefix
from ingestion.storage import LocalStorageBackend, StorageFactory, land
from tests.ingestion.converters.conftest import zip_of
from tests.ingestion.extractors.conftest import FakeHttpServer, Route

FIRST = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
SECOND = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)


def _ingest_bacen(
    storage: LocalStorageBackend, server: FakeHttpServer, when: datetime, work: Path
) -> str:
    config = ExtractorConfig(
        source="api",
        conn_id="http_test",
        requests=(
            HttpRequest(name="ipca", endpoint="/dados/serie/bcdata.sgs.433/dados"),
            HttpRequest(name="selic_meta", endpoint="/dados/serie/bcdata.sgs.432/dados"),
        ),
    )
    partition = ingestion_partition(when)
    raw = raw_prefix("bacen", "sgs", partition)
    land(storage, ExtractorFactory.create(config, ingestion_time=when).extract(work), raw)
    staging = staging_prefix("bacen", "sgs", partition)
    convert_partition(
        storage,
        raw_prefix=raw,
        staging_prefix=staging,
        latest_prefix="staging/bacen/sgs/latest/",
        config=ConverterConfig(),
        work_dir=work,
    )
    return staging


def _bronze(storage: LocalStorageBackend, glob: str) -> duckdb.DuckDBPyRelation:
    return duckdb.sql(
        f"select * from read_parquet('{storage.root}/{glob}', filename => true)"
    )


@pytest.fixture
def bacen_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeHttpServer]:
    with FakeHttpServer() as server:
        monkeypatch.setenv("AIRFLOW_CONN_HTTP_TEST", f"http://127.0.0.1:{server.port}")
        yield server


def test_bacen_series_reach_the_bronze_select_as_text_with_their_origin(
    bacen_server: FakeHttpServer, tmp_path: Path
) -> None:
    storage = StorageFactory.create("local", root=tmp_path / "lake")
    assert isinstance(storage, LocalStorageBackend)
    bacen_server.routes["/dados/serie/bcdata.sgs.433/dados"] = Route(
        body=b'[{"data":"01/07/2026","valor":"0.26"},{"data":"01/08/2026","valor":"-0.32"}]'
    )
    bacen_server.routes["/dados/serie/bcdata.sgs.432/dados"] = Route(
        body=b'[{"data":"08/10/2026","valor":"13.75"}]'
    )

    _ingest_bacen(storage, bacen_server, FIRST, tmp_path / "work")
    bronze = _bronze(storage, "staging/bacen/sgs/latest/*/*/*.parquet")

    assert bronze.columns == ["data", "valor", "filename"]
    assert bronze.types[:2] == ["VARCHAR", "VARCHAR"]
    rows = sorted(
        (Path(filename).stem, data, valor) for data, valor, filename in bronze.fetchall()
    )
    assert rows == [
        ("ipca", "01/07/2026", "0.26"),
        ("ipca", "01/08/2026", "-0.32"),
        ("selic_meta", "08/10/2026", "13.75"),
    ]


def test_latest_shows_only_the_last_ingestion_and_dated_glob_shows_all(
    bacen_server: FakeHttpServer, tmp_path: Path
) -> None:
    storage = StorageFactory.create("local", root=tmp_path / "lake")
    assert isinstance(storage, LocalStorageBackend)
    route = bacen_server.routes["/dados/serie/bcdata.sgs.433/dados"] = Route(
        body=b'[{"data":"01/08/2026","valor":"-0.32"}]'
    )
    bacen_server.routes["/dados/serie/bcdata.sgs.432/dados"] = Route(body=b'[{"v":"1"}]')
    _ingest_bacen(storage, bacen_server, FIRST, tmp_path / "work")

    # Revisão da fonte: o mesmo mês com outro valor, e um mês novo.
    route.body = (
        b'[{"data":"01/08/2026","valor":"-0.30"},{"data":"01/09/2026","valor":"0.48"}]'
    )
    _ingest_bacen(storage, bacen_server, SECOND, tmp_path / "work")

    overwrite = _bronze(storage, "staging/bacen/sgs/latest/*/*/ipca.parquet")
    published = f"{storage.root}/staging/bacen/sgs/latest/{ingestion_partition(SECOND)}"
    assert overwrite.fetchall() == [
        ("01/08/2026", "-0.30", f"{published}/ipca.parquet"),
        ("01/09/2026", "0.48", f"{published}/ipca.parquet"),
    ]
    history = _bronze(storage, "staging/bacen/sgs/2*/*/ipca.parquet")
    assert len(history.fetchall()) == 3


def test_tesouro_zip_with_utf16_tsv_reaches_the_bronze_select(tmp_path: Path) -> None:
    storage = StorageFactory.create("local", root=tmp_path / "lake")
    assert isinstance(storage, LocalStorageBackend)
    preamble = "".join(f"Tesouro Gerencial, linha {n}\n" for n in range(12))
    tsv = (preamble + "Unidade\tDotação Atualizada\n53000\t1.234,56\n").encode("utf-16")
    partition = ingestion_partition(SECOND)
    raw = raw_prefix("siafi-tesouro-gerencial", "dotacao_execucao", partition)
    zipped = write_stream(
        [zip_of({"dotacao.csv": tsv})], tmp_path / "anexo" / "dotacao_execucao.zip"
    )
    land(storage, [zipped], raw)

    convert_partition(
        storage,
        raw_prefix=raw,
        staging_prefix=staging_prefix(
            "siafi-tesouro-gerencial", "dotacao_execucao", partition
        ),
        latest_prefix="staging/siafi-tesouro-gerencial/dotacao_execucao/latest/",
        config=ConverterConfig(encoding="utf-16", delimiter="\t", skip_rows=12),
        work_dir=tmp_path / "work",
    )
    bronze = _bronze(
        storage, "staging/siafi-tesouro-gerencial/dotacao_execucao/latest/*/*/*.parquet"
    )

    assert bronze.columns[:2] == ["Unidade", "Dotação Atualizada"]
    assert [row[:2] for row in bronze.fetchall()] == [("53000", "1.234,56")]
