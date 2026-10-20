"""Estratégia `http_session`: um fluxo de passos numa mesma sessão HTTP."""

import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import pytest

from ingestion.extractors import ExtractionError, ExtractorConfig, ExtractorFactory
from ingestion.extractors.session import (
    Contains,
    Cookie,
    Download,
    DropHeaders,
    Header,
    HttpSession,
    JsonEquals,
    JsonField,
    Regex,
    Request,
    SetHeaders,
)
from ingestion.extractors.session import session_extractor
from tests.ingestion.extractors.conftest import WHEN, FakeHttpServer, Route


def _run(
    server: FakeHttpServer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *steps: Any,
    variables: dict[str, str] | None = None,
) -> dict[str, bytes]:
    values = {"usuario_var": "eu@exemplo.org", "senha_var": "segredo"}
    monkeypatch.setattr(session_extractor, "read_variable", values.__getitem__)
    config = ExtractorConfig(
        source="http_session",
        session=HttpSession(steps=steps, variables=variables or {}),
    )
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    return {part.name: part.path.read_bytes() for part in extractor.extract(tmp_path)}


def _url(server: FakeHttpServer, path: str) -> str:
    return f"http://127.0.0.1:{server.port}{path}"


def test_login_flow_with_captures_templates_and_variables(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = _url(http_server, "")
    http_server.routes["/versao"] = Route(body=b'{"token": "v9"}')
    http_server.routes["/inicio"] = Route(
        body=b'<input id="__STATE" value="abc&amp;1"/>',
        headers={"Set-Cookie": "sessao=crf%3Dtok%3Bx%3D1; Path=/", "X-Id": "77"},
    )
    login = http_server.routes["/login"] = Route(
        on_post=Route(body=json.dumps({"ok": True, "next": f"{base}/dados"}).encode())
    )
    download = http_server.routes["/dados"] = Route(
        on_post=Route(body=b"a;b\n1;2\n", headers={"Content-Type": "text/csv"})
    )

    files = _run(
        http_server,
        tmp_path,
        monkeypatch,
        Request("versao", "GET", f"{base}/versao", capture={"v": JsonField("token")}),
        Request(
            "inicio",
            "GET",
            f"{base}/inicio",
            capture={
                "state": Regex(r'id="__STATE" value="(.*?)"', unescape_html=True),
                "csrf": Cookie("sessao", pattern=r"crf=([^;]+)", unquote_url=True),
                "id": Header("X-Id"),
            },
        ),
        SetHeaders({"X-CSRF": "{csrf}", "X-Apagar": "1"}),
        Request(
            "login",
            "POST",
            f"{base}/login",
            json={"usuario": "{usuario}", "senha": "{senha}", "versao": "{v}", "n": 0},
            expect=(JsonEquals("ok", True),),
            capture={"proximo": JsonField("next")},
        ),
        DropHeaders(("X-Apagar",)),
        Download(
            "csv",
            "POST",
            "{proximo}",
            data={"state": "{state}", "id": "{id}", "opcional": ""},
            drop_empty=("opcional",),
            filename="dados.csv",
            content_types=("text/csv",),
        ),
        variables={"usuario": "usuario_var", "senha": "senha_var"},
    )

    assert files == {"dados.csv": b"a;b\n1;2\n"}
    assert login.on_post is not None
    sent = login.on_post.requests[0]
    assert json.loads(sent["body"]) == {
        "usuario": "eu@exemplo.org",
        "senha": "segredo",
        "versao": "v9",
        "n": 0,
    }
    assert sent["headers"]["X-CSRF"] == "tok"
    assert download.on_post is not None
    final = download.on_post.requests[0]
    assert parse_qs(final["body"].decode()) == {"state": ["abc&1"], "id": ["77"]}
    assert "X-Apagar" not in final["headers"]


def test_missing_capture_uses_the_default_or_fails_with_the_step_name(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    http_server.routes["/x"] = Route(body=b"nada aqui")
    http_server.routes["/arquivo"] = Route(
        body=b"ok", headers={"Content-Type": "text/csv"}
    )
    url = _url(http_server, "/x")

    files = _run(
        http_server,
        tmp_path,
        monkeypatch,
        Request("pagina", "GET", url, capture={"v": Regex("v=(\\d+)", default="1")}),
        Download(
            "baixa", "GET", _url(http_server, "/arquivo") + "?v={v}", filename="a.csv"
        ),
    )
    assert files == {"a.csv": b"ok"}

    with pytest.raises(ExtractionError, match="pagina.*v"):
        _run(
            http_server,
            tmp_path,
            monkeypatch,
            Request("pagina", "GET", url, capture={"v": Regex("v=(\\d+)")}),
            Download("baixa", "GET", url, filename="a.csv"),
        )


def test_optional_capture_keeps_the_previous_value(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    http_server.routes["/a"] = Route(body=b"v=1")
    http_server.routes["/b"] = Route(body=b"sem valor")
    arquivo = http_server.routes["/arquivo"] = Route(body=b"ok")

    _run(
        http_server,
        tmp_path,
        monkeypatch,
        Request("a", "GET", _url(http_server, "/a"), capture={"v": Regex("v=(\\d)")}),
        Request(
            "b",
            "GET",
            _url(http_server, "/b"),
            capture={"v": Regex("v=(\\d)", required=False)},
        ),
        Download("baixa", "GET", _url(http_server, "/arquivo") + "?v={v}", filename="a"),
    )

    assert arquivo.requests[0]["query"] == "v=1"


def test_failed_check_and_http_error_name_the_step(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    http_server.routes["/busca"] = Route(body=b"resultado vazio")
    http_server.routes["/proibido"] = Route(status=403)

    with pytest.raises(ExtractionError, match="busca.*ICST"):
        _run(
            http_server,
            tmp_path,
            monkeypatch,
            Request(
                "busca", "GET", _url(http_server, "/busca"), expect=(Contains("ICST"),)
            ),
            Download("baixa", "GET", _url(http_server, "/busca"), filename="a"),
        )
    with pytest.raises(ExtractionError, match="entrada.*403"):
        _run(
            http_server,
            tmp_path,
            monkeypatch,
            Request("entrada", "GET", _url(http_server, "/proibido")),
            Download("baixa", "GET", _url(http_server, "/busca"), filename="a"),
        )


def test_status_can_be_ignored_for_best_effort_steps(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    http_server.routes["/versao"] = Route(status=500)
    http_server.routes["/arquivo"] = Route(body=b"ok")

    files = _run(
        http_server,
        tmp_path,
        monkeypatch,
        Request(
            "versao",
            "GET",
            _url(http_server, "/versao"),
            check_status=False,
            capture={"v": JsonField("token", default="padrao")},
        ),
        Download("baixa", "GET", _url(http_server, "/arquivo") + "?v={v}", filename="a"),
    )

    assert files == {"a": b"ok"}
    assert http_server.routes["/arquivo"].requests[0]["query"] == "v=padrao"


def test_download_with_unexpected_content_type_is_an_error(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    http_server.routes["/arquivo"] = Route(
        body=b"<html>erro</html>", headers={"Content-Type": "text/html"}
    )

    with pytest.raises(ExtractionError, match="csv.*text/html"):
        _run(
            http_server,
            tmp_path,
            monkeypatch,
            Download(
                "csv",
                "GET",
                _url(http_server, "/arquivo"),
                filename="a.csv",
                content_types=("text/csv",),
            ),
        )


def test_flow_without_download_is_rejected() -> None:
    config = ExtractorConfig(
        source="http_session",
        session=HttpSession(steps=(Request("a", "GET", "http://x"),)),
    )

    with pytest.raises(ValueError, match="Download"):
        ExtractorFactory.create(config, ingestion_time=WHEN)
