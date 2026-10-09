"""Resolvedores de URL: o link da edição corrente, achado na fonte."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from ingestion.extractors import (
    ExtractionError,
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
)
from ingestion.extractors.resolvers import link_in_page, mziq_latest_file
from ingestion.layout import TIMEZONE
from tests.ingestion.extractors.conftest import WHEN, FakeHttpServer, Route

PAGE = b"""<html><body>
<a href="/outro.pdf">Relatorio</a>
<a href="/download?file=cp-historico-agosto2026.xlsx">Historico</a>
<a href="/download?file=cp-historico-julho2026.xlsx">Anterior</a>
</body></html>"""


def _download(server: FakeHttpServer, tmp_path: Path, contains: str) -> list[str]:
    config = ExtractorConfig(
        source="http_file",
        conn_id="http_test",
        requests=(
            HttpRequest(
                name="poupanca",
                endpoint="/indicadores/poupanca",
                resolve=link_in_page(
                    "/indicadores/poupanca", contains, headers={"User-Agent": "UA"}
                ),
            ),
        ),
    )
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    return [part.path.read_text() for part in extractor.extract(tmp_path)]


def test_downloads_the_first_link_that_contains_the_pattern(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    page = http_server.routes["/indicadores/poupanca"] = Route(body=PAGE)
    http_server.routes["/download"] = Route(body=b"xlsx de agosto")

    assert _download(http_server, tmp_path, "cp-historico") == ["xlsx de agosto"]
    assert page.requests[0]["headers"]["User-Agent"] == "UA"
    download = http_server.routes["/download"].requests[0]
    assert download["query"] == "file=cp-historico-agosto2026.xlsx"


def test_page_without_the_link_is_an_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    http_server.routes["/indicadores/poupanca"] = Route(body=PAGE)

    with pytest.raises(ExtractionError, match="unidades.*/indicadores/poupanca"):
        _download(http_server, tmp_path, "unidades")


def test_page_that_fails_is_an_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    http_server.routes["/indicadores/poupanca"] = Route(status=403)

    with pytest.raises(ExtractionError, match="403"):
        _download(http_server, tmp_path, "cp-historico")


MZIQ = "/filemanager/company/c1/filter/categories/year/meta"


def _mziq(server: FakeHttpServer, tmp_path: Path) -> list[str]:
    config = ExtractorConfig(
        source="http_file",
        conn_id="http_test",
        requests=(
            HttpRequest(
                name="planilha",
                endpoint=MZIQ,
                resolve=mziq_latest_file("c1", "planilha_interativa"),
            ),
        ),
    )
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    return [part.path.read_text() for part in extractor.extract(tmp_path)]


def test_mziq_downloads_the_latest_quarter_of_the_most_recent_year(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    base = f"http://127.0.0.1:{http_server.port}"
    documents = [
        {"internal_name": "planilha_interativa", "file_quarter": q, "permalink": link}
        for q, link in ((2, f"{base}/files/2t"), (3, f"{base}/files/3t"))
    ] + [{"internal_name": "outra", "file_quarter": 4, "permalink": f"{base}/x"}]
    api = http_server.routes[MZIQ] = Route(
        on_post=Route(body=json.dumps({"data": {"document_metas": documents}}).encode())
    )
    http_server.routes["/files/3t"] = Route(body=b"planilha 3T")

    assert _mziq(http_server, tmp_path) == ["planilha 3T"]
    assert api.on_post is not None
    payload = json.loads(api.on_post.requests[0]["body"])
    assert payload["categories"] == ["planilha_interativa"]
    assert payload["published"] is True


def test_mziq_without_documents_in_three_years_is_an_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    api = http_server.routes[MZIQ] = Route(
        on_post=Route(body=b'{"data": {"document_metas": []}}')
    )

    with pytest.raises(ExtractionError, match="planilha_interativa"):
        _mziq(http_server, tmp_path)
    assert api.on_post is not None
    year = datetime.now(TIMEZONE).year
    assert [json.loads(r["body"])["year"] for r in api.on_post.requests] == [
        str(year),
        str(year - 1),
        str(year - 2),
    ]
