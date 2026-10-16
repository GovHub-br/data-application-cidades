"""Resolvedores de URL: o link da edição corrente, achado na fonte."""

import json
from pathlib import Path
from typing import Any

import pytest

from ingestion.extractors import (
    ExtractionError,
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
)
from ingestion.extractors.resolvers import latest_in_json_listing, link_in_page
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


LISTING = "/catalogo"


def _latest(server: FakeHttpServer, tmp_path: Path, resolve: Any) -> list[str]:
    config = ExtractorConfig(
        source="http_file",
        conn_id="http_test",
        requests=(HttpRequest(name="arquivo", endpoint=LISTING, resolve=resolve),),
    )
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    return [part.path.read_text() for part in extractor.extract(tmp_path)]


def _documents(base: str) -> bytes:
    documents = [
        {"tipo": "planilha", "trimestre": q, "link": f"{base}/files/{q}t"}
        for q in (2, 3)
    ] + [{"tipo": "outra", "trimestre": 4, "link": f"{base}/x"}]
    return json.dumps({"data": {"docs": documents}}).encode()


def test_post_listing_picks_the_highest_matching_item(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    base = f"http://127.0.0.1:{http_server.port}"
    api = http_server.routes[LISTING] = Route(on_post=Route(body=_documents(base)))
    http_server.routes["/files/3t"] = Route(body=b"terceiro trimestre")
    resolve = latest_in_json_listing(
        LISTING,
        method="POST",
        json={"categorias": ["planilha"], "publicado": True},
        items="data.docs",
        where={"tipo": "planilha"},
        order_by="trimestre",
        pick="link",
        attempts=({"ano": "2026"},),
    )

    assert _latest(http_server, tmp_path, resolve) == ["terceiro trimestre"]
    assert api.on_post is not None
    assert json.loads(api.on_post.requests[0]["body"]) == {
        "categorias": ["planilha"],
        "publicado": True,
        "ano": "2026",
    }


def test_attempts_are_tried_in_order_until_an_item_matches(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    api = http_server.routes[LISTING] = Route(body=b'{"data": {"docs": []}}')
    resolve = latest_in_json_listing(
        LISTING,
        params={"lingua": "pt"},
        items="data.docs",
        pick="link",
        attempts=({"ano": "2026"}, {"ano": "2025"}),
    )

    with pytest.raises(ExtractionError, match="data.docs"):
        _latest(http_server, tmp_path, resolve)
    assert [r["query"] for r in api.requests] == [
        "lingua=pt&ano=2026",
        "lingua=pt&ano=2025",
    ]


def test_without_order_by_the_first_matching_item_wins(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    base = f"http://127.0.0.1:{http_server.port}"
    http_server.routes[LISTING] = Route(body=_documents(base))
    http_server.routes["/files/2t"] = Route(body=b"segundo trimestre")
    resolve = latest_in_json_listing(LISTING, items="data.docs", pick="link")

    assert _latest(http_server, tmp_path, resolve) == ["segundo trimestre"]
