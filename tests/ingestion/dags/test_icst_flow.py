"""Fluxo do ICST declarado na DAG, executado pelo `http_session` contra um portal
falso que reproduz as etapas do OutSystems e do FGVDados (ASP.NET)."""

import json
from pathlib import Path
from urllib.parse import parse_qs

import pytest

from ingestion.extractors import ExtractionError, ExtractorConfig, ExtractorFactory
from ingestion.extractors.session import session_extractor
from tests.ingestion.dags.conftest import load_dag_module
from tests.ingestion.extractors.conftest import WHEN, FakeHttpServer, Route

AUTH = "/ProdutosDigitais/"
LEGACY = "/IBRE/sitefgvdados/"
SCREEN = "screenservices/ProdutosDigitais/Blocks/BL01_Login/"
STATES = (
    b'<input id="__VIEWSTATE" value="vs"/>'
    b'<input id="__VIEWSTATEGENERATOR" value="gen"/>'
    b'<input id="__EVENTVALIDATION" value="ev"/>'
)
CSV = "Data;ICST com ajuste;ICST sem ajuste\n01/2026;90,1;89,5\n".encode("latin-1")


def _portal(
    server: FakeHttpServer, *, login_ok: bool = True, download: Route | None = None
) -> dict[str, Route]:
    base = f"http://127.0.0.1:{server.port}"
    routes = {
        AUTH + "moduleservices/moduleversioninfo": Route(body=b'{"versionToken": "mv"}'),
        AUTH
        + "scripts/ProdutosDigitais.Blocks.BL01_Login.mvc.js": Route(
            body=(
                f'callDataAction("a", "{SCREEN}DataActionCheckUsarCloudFlare", "hcf");'
                f'callDataAction("b", "{SCREEN}DataActionGetDadosLogin", "hlogin");'
            ).encode()
        ),
        AUTH: Route(body=b"<html></html>"),
        AUTH
        + SCREEN
        + "DataActionCheckUsarCloudFlare": Route(
            body=b"{}", headers={"Set-Cookie": "nr2Users=crf%3Dtok%3Buid%3D1; Path=/"}
        ),
        AUTH
        + SCREEN
        + "DataActionGetDadosLogin": Route(
            body=json.dumps(
                {
                    "data": {
                        "FLG_Sucesso": login_ok,
                        "URL_Gratuito": f"{base}{LEGACY}Default.aspx?token=1",
                    }
                }
            ).encode()
        ),
        LEGACY + "Default.aspx": Route(body=STATES, on_post=Route(body=b"1|ICST|")),
        LEGACY + "consulta.aspx": Route(body=STATES + b"ICST", on_post=Route(body=b"")),
        LEGACY + "visualizaconsulta.aspx": Route(body=b"<html></html>"),
        LEGACY
        + "VisualizaConsultaFrame.aspx": Route(
            body=STATES + b"ICST",
            on_post=download
            or Route(body=CSV, headers={"Content-Type": "text/csv; charset=iso-8859-1"}),
        ),
    }
    server.routes.update(routes)
    return routes


def _extract(
    server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> list[bytes]:
    variables = {"dados_fgv_email": "eu@exemplo.org", "dados_fgv_password": "segredo"}
    monkeypatch.setattr(session_extractor, "read_variable", variables.__getitem__)
    base = f"http://127.0.0.1:{server.port}"
    module = load_dag_module("data_ingest/fgv/icst_ingest_dag.py")
    config = ExtractorConfig(
        source="http_session", session=module.fluxo(base + AUTH, base + LEGACY)
    )
    extractor = ExtractorFactory.create(config, ingestion_time=WHEN)
    parts = list(extractor.extract(tmp_path))
    assert [part.name for part in parts] == ["icst.csv"]
    return [part.path.read_bytes() for part in parts]


def test_downloads_the_series_csv_as_the_portal_sent_it(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    routes = _portal(http_server)

    assert _extract(http_server, tmp_path, monkeypatch) == [CSV]

    login = routes[AUTH + SCREEN + "DataActionGetDadosLogin"].requests[0]
    variables = json.loads(login["body"])["screenData"]["variables"]
    assert (variables["DSLogin"], variables["DSPassword"]) == (
        "eu@exemplo.org",
        "segredo",
    )
    assert json.loads(login["body"])["versionInfo"] == {
        "moduleVersion": "mv",
        "apiVersion": "hlogin",
    }
    assert login["headers"]["X-CSRFToken"] == "tok"
    search = routes[LEGACY + "Default.aspx"].on_post
    assert search is not None
    form = parse_qs(search.requests[0]["body"].decode())
    assert form["ctl00$txtBuscarSeries"] == ["ICST"]
    assert form["__VIEWSTATE"] == ["vs"]


def test_rejected_login_is_an_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _portal(http_server, login_ok=False)

    with pytest.raises(ExtractionError, match="login.*FLG_Sucesso"):
        _extract(http_server, tmp_path, monkeypatch)


def test_page_instead_of_csv_is_an_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _portal(
        http_server,
        download=Route(body=b"<html>erro</html>", headers={"Content-Type": "text/html"}),
    )

    with pytest.raises(ExtractionError, match="csv.*text/html"):
        _extract(http_server, tmp_path, monkeypatch)


def test_series_missing_from_the_search_is_an_extraction_error(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    routes = _portal(http_server)
    routes[LEGACY + "Default.aspx"].on_post = Route(body=b"1|nada|")

    with pytest.raises(ExtractionError, match="busca.*ICST"):
        _extract(http_server, tmp_path, monkeypatch)


def test_unknown_portal_versions_fall_back_to_the_last_known_deploy(
    http_server: FakeHttpServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    routes = _portal(http_server)
    del http_server.routes[AUTH + "moduleservices/moduleversioninfo"]
    http_server.routes[AUTH + "scripts/ProdutosDigitais.Blocks.BL01_Login.mvc.js"] = (
        Route(body=b"sem chamadas")
    )
    # sem as versões descobertas, o fluxo usa os caminhos padrão do login
    http_server.routes[AUTH + SCREEN + "DataActionCheckUsarCloudFlare"] = routes[
        AUTH + SCREEN + "DataActionCheckUsarCloudFlare"
    ]

    assert _extract(http_server, tmp_path, monkeypatch) == [CSV]
    login = routes[AUTH + SCREEN + "DataActionGetDadosLogin"].requests[0]
    assert json.loads(login["body"])["versionInfo"] == {
        "moduleVersion": "vuthrRMgPWqGaqAin6KHTA",
        "apiVersion": "kEIaQNU5n93i9Q026f_dlQ",
    }
