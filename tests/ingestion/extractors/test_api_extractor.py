"""O que é próprio do extrator `api`, além do contrato."""

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
from tests.ingestion.extractors.conftest import WHEN, FakeHttpServer, Route


def _extract(tmp_path: Path, *requests: HttpRequest) -> list[bytes]:
    config = ExtractorConfig(source="api", conn_id="http_test", requests=requests)
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    return [part.path.read_bytes() for part in extractor.extract(tmp_path)]


def test_get_sends_params_as_query_string(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    route = http_server.routes["/dados"] = Route(body=b"[]")

    _extract(
        tmp_path,
        HttpRequest(
            name="s", endpoint="/dados", params={"formato": "json", "ultimos": 13}
        ),
    )

    assert route.requests[0]["query"] == "formato=json&ultimos=13"


def test_post_sends_json_body(http_server: FakeHttpServer, tmp_path: Path) -> None:
    route = http_server.routes["/querydata"] = Route(body=b'{"results": []}')
    body: dict[str, Any] = {"queries": [{"Query": {"Commands": []}}]}

    _extract(
        tmp_path, HttpRequest(name="q", endpoint="/querydata", method="POST", json=body)
    )

    assert route.requests[0]["method"] == "POST"
    assert json.loads(route.requests[0]["body"]) == body


def test_server_error_is_retried(http_server: FakeHttpServer, tmp_path: Path) -> None:
    route = http_server.routes["/instavel"] = Route(body=b"ok", fail_first=2)

    assert _extract(tmp_path, HttpRequest(name="i", endpoint="/instavel")) == [b"ok"]
    assert route.calls == 3


def test_server_error_that_persists_becomes_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    http_server.routes["/fora"] = Route(fail_first=99)

    with pytest.raises(ExtractionError, match="503"):
        _extract(tmp_path, HttpRequest(name="f", endpoint="/fora"))


def test_html_instead_of_json_is_retried(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    # O SGS do BACEN às vezes responde 200 com a página "Requisição inválida!".
    route = http_server.routes["/sgs"] = Route(
        body=b"[]", headers={"Content-Type": "application/json"}, html_first=2
    )

    assert _extract(tmp_path, HttpRequest(name="s", endpoint="/sgs")) == [b"[]"]
    assert route.calls == 3


def test_html_that_persists_becomes_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    http_server.routes["/sgs"] = Route(html_first=99)

    with pytest.raises(ExtractionError, match="não é JSON.*text/html"):
        _extract(tmp_path, HttpRequest(name="s", endpoint="/sgs"))


def test_client_error_is_not_retried(http_server: FakeHttpServer, tmp_path: Path) -> None:
    route = http_server.routes["/proibido"] = Route(status=403)

    with pytest.raises(ExtractionError, match="403"):
        _extract(tmp_path, HttpRequest(name="p", endpoint="/proibido"))
    assert route.calls == 1


def test_config_without_requests_is_rejected() -> None:
    with pytest.raises(ValueError, match="requests"):
        ExtractorFactory.create(
            ExtractorConfig(source="api", conn_id="http_test"), ingestion_time=WHEN
        )
