"""Resolvedores de URL: o link da edição corrente, achado na página da fonte."""

from pathlib import Path

import pytest

from ingestion.extractors import (
    ExtractionError,
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
)
from ingestion.extractors.resolvers import link_in_page
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
