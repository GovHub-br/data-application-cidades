"""Estratégia `fgvdados`: série do portal FGVDados (IBRE), em CSV, depois do login.

O portal não tem API: o CSV só sai depois de logar no OutSystems
(`autenticacao-ibre`) e navegar no FGVDados (`extra-ibre`, ASP.NET WebForms),
que guarda o estado da tela em campos ocultos e exige a mesma sessão do começo ao
fim. Por isso a estratégia usa uma `requests.Session` própria, e não o HttpHook
(que abre sessão nova a cada chamada).

Etapas, cada uma com o erro dizendo onde parou:

1. versões do OutSystems (hashes que mudam a cada deploy do portal);
2. login (CSRF do cookie `nr2Users`) → URL de entrada no FGVDados;
3. busca e seleção da série;
4. consulta da série histórica;
5. download do CSV, gravado como veio (`<série>.csv`, latin-1 e `;`).
"""

import html
import logging
import re
import urllib.parse
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from airflow.sdk import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

from ingestion.extractors.base_extractor import Extractor, RawFile, write_stream
from ingestion.extractors.config_extractor import ExtractorConfig, FgvDadosQuery
from ingestion.extractors.extractor_errors import ExtractionError
from ingestion.extractors.extractor_registry import ExtractorFactory

TIMEOUT_SECONDS = 120
CHUNK_BYTES = 1024 * 1024
LOGIN_SCREEN = "screenservices/ProdutosDigitais/Blocks/BL01_Login/"
# Valores do último deploy conhecido; valem se a descoberta falhar.
FALLBACK_VERSIONS = {
    "moduleVersion": "vuthrRMgPWqGaqAin6KHTA",
    "api_cloudflare": "OTGxLOOIYRPT4Yzxi6zCuA",
    "api_login": "kEIaQNU5n93i9Q026f_dlQ",
    "url_cloudflare": LOGIN_SCREEN + "DataActionCheckUsarCloudFlare",
    "url_login": LOGIN_SCREEN + "DataActionGetDadosLogin",
}
DATA_ACTIONS = {
    "DataActionCheckUsarCloudFlare": ("api_cloudflare", "url_cloudflare"),
    "DataActionGET_UsarCloudFlare": ("api_cloudflare", "url_cloudflare"),
    "DataActionGetDadosLogin": ("api_login", "url_login"),
    "DataActionGET_Dados": ("api_login", "url_login"),
}
CALL_DATA_ACTION = re.compile(
    r'callDataAction\(\s*"[^"]*"\s*,\s*"(screenservices/[^"]*/(DataAction\w+))"'
    r'\s*,\s*"([^"]+)"'
)
CSV_TYPES = ("text/csv", "application/octet-stream")


class LegacySSLAdapter(HTTPAdapter):
    """Baixa o nível do OpenSSL para o IIS legado do FGVDados."""

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        context = create_urllib3_context()
        context.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = context
        super().init_poolmanager(*args, **kwargs)


def read_variable(name: str) -> str:
    """Variable do Airflow, lida em runtime (nunca no parse da DAG)."""
    return str(Variable.get(name))


@ExtractorFactory.register("fgvdados")
class FgvDadosExtractor(Extractor):
    """Faz o login, navega até a série e grava o CSV em `<série>.csv`."""

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if config.fgvdados is None:
            raise ValueError("a estratégia fgvdados precisa de config.fgvdados")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        query: FgvDadosQuery = self.config.fgvdados  # type: ignore[assignment]
        portal = _Portal(query)
        try:
            entry = portal.login(
                read_variable(query.email_variable),
                read_variable(query.password_variable),
            )
            portal.select_series(entry)
            portal.open_history()
            response = portal.download_csv()
        except requests.RequestException as exc:
            raise ExtractionError(f"fgvdados ({query.series}): {exc}") from exc
        name = f"{query.series.lower()}.csv"
        try:
            yield write_stream(response.iter_content(CHUNK_BYTES), work_dir / name, name)
        finally:
            response.close()


class _Portal:
    """Uma sessão do portal, do login ao download."""

    def __init__(self, query: FgvDadosQuery) -> None:
        self.query = query
        self.session = requests.Session()
        legacy = urllib.parse.urlsplit(query.legacy_url)
        self.session.mount(f"{legacy.scheme}://{legacy.netloc}", LegacySSLAdapter())

    def _fail(self, step: str) -> ExtractionError:
        return ExtractionError(f"fgvdados ({self.query.series}): {step}")

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.session.get(url, timeout=TIMEOUT_SECONDS, **kwargs)

    def _post(self, url: str, **kwargs: Any) -> requests.Response:
        return self.session.post(url, timeout=TIMEOUT_SECONDS, **kwargs)

    # 1. versões do OutSystems
    def _versions(self) -> dict[str, str]:
        versions = dict(FALLBACK_VERSIONS)
        # Com Accept: application/json o servidor recusa o .js (406).
        plain = {"Accept": "*/*"}
        auth = self.query.auth_url
        try:
            info = self._get(auth + "moduleservices/moduleversioninfo", headers=plain)
            if info.ok and info.json().get("versionToken"):
                versions["moduleVersion"] = info.json()["versionToken"]
            script = self._get(
                auth + "scripts/ProdutosDigitais.Blocks.BL01_Login.mvc.js",
                headers=plain,
            )
            if script.ok:
                for match in CALL_DATA_ACTION.finditer(script.text):
                    keys = DATA_ACTIONS.get(match.group(2))
                    if keys:
                        versions[keys[0]] = match.group(3)
                        versions[keys[1]] = match.group(1)
        except (requests.RequestException, ValueError) as exc:
            logging.warning("fgvdados: versões do OutSystems indisponíveis (%s)", exc)
        return versions

    # 2. login
    def login(self, email: str, password: str) -> str:
        """Faz o login e devolve a URL de entrada no FGVDados."""
        versions = self._versions()
        auth = self.query.auth_url
        origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(auth))
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) "
                    "Gecko/20100101 Firefox/149.0"
                ),
                "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json",
                "OutSystems-Client-Type": "Web",
                "Origin": origin,
                "Referer": auth,
                "X-CSRFToken": "",
            }
        )
        self._get(auth)
        self._post(
            auth + versions["url_cloudflare"],
            json=_login_payload(versions, versions["api_cloudflare"], "", ""),
        )
        cookie = self.session.cookies.get("nr2Users")
        if not cookie or "crf=" not in urllib.parse.unquote(cookie):
            raise self._fail("login sem o token CSRF (cookie nr2Users)")
        token = urllib.parse.unquote(cookie).split("crf=")[1].split(";")[0]
        self.session.headers["X-CSRFToken"] = token
        answer = self._post(
            auth + versions["url_login"],
            json=_login_payload(versions, versions["api_login"], email, password),
        ).json()
        data = answer.get("data") or {}
        if not data.get("FLG_Sucesso"):
            raise self._fail("login recusado (credenciais das Variables)")
        for header in (
            "Content-Type",
            "Accept",
            "OutSystems-Client-Type",
            "Origin",
            "Referer",
            "X-CSRFToken",
        ):
            self.session.headers.pop(header, None)
        return str(data["URL_Gratuito"])

    # 3. busca e seleção da série
    def select_series(self, entry: str) -> None:
        page = self.query.legacy_url + "Default.aspx"
        states = _states(self._get(entry).text)
        ajax = _ajax_headers(page)
        series = self.query.series
        form = {
            "ctl00$drpFiltro": "E",
            "ctl00$txtBuscarSeries": series,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": states["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
        }
        found = self._post(
            page,
            data={
                **form,
                "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeries",
                "ctl00$butBuscarSeries": "OK",
            },
            headers=ajax,
        )
        if series not in found.text:
            raise self._fail(f"série {series} não encontrada na busca")
        states.update(_delta_states(found.text))
        self._post(
            page,
            data={
                **form,
                "__VIEWSTATE": states["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
                "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeriesOK",
                "ctl00$dlsSerie$ctl00$chkSerieEscolhida": "on",
                "ctl00$dlsSerie$ctl01$chkSerieEscolhida": "on",
                "ctl00$butBuscarSeriesOK": "OK",
            },
            headers=ajax,
        )

    # 4. consulta da série histórica
    def open_history(self) -> None:
        page = self.query.legacy_url + "consulta.aspx"
        current = self._get(page)
        if self.query.series not in current.text:
            raise self._fail("a série não ficou na sessão da consulta")
        states = _states(current.text)
        ajax = _ajax_headers(page)
        form = {
            "ctl00$drpFiltro": "E",
            "ctl00$txtBuscarSeries": "",
            "ctl00$cphConsulta$rblConsultaHierarquia": "COMPARATIVA",
            "ctl00$cphConsulta$cpeLegenda_ClientState": "false",
            "ctl00$cphConsulta$chkEscolhida": "on",
            "ctl00$cphConsulta$dlsSerie$ctl00$chkSerieEscolhida": "on",
            "ctl00$cphConsulta$dlsSerie$ctl01$chkSerieEscolhida": "on",
            "ctl00$cphConsulta$gnResultado": "rbtSerieHistorica",
            "ctl00$cphConsulta$txtMes": "__/__/____",
            "ctl00$cphConsulta$mkeMes_ClientState": "",
            "ctl00$cphConsulta$txtPeriodoInicio": "__/__/____",
            "ctl00$cphConsulta$mkePeriodoInicio_ClientState": "",
            "ctl00$cphConsulta$txtPeriodoFim": "__/__/____",
            "ctl00$cphConsulta$mkePeriodoFim_ClientState": "",
            "ctl00$txtBAPalavraChave": "",
            "ctl00$rblTipoTexto": "E",
            "ctl00$txtBAColuna": "",
            "ctl00$txtBAIncluida": "",
            "ctl00$txtBAAtualizada": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
        }
        radio = self._post(
            page,
            data={
                **form,
                "ctl00$smg": (
                    "ctl00$cphConsulta$updpOpcoes|ctl00$cphConsulta$rbtSerieHistorica"
                ),
                "__EVENTTARGET": "ctl00$cphConsulta$rbtSerieHistorica",
                "__VIEWSTATE": states["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
            },
            headers=ajax,
        )
        states.update(_delta_states(radio.text))
        show = {
            **form,
            "ctl00$smg": (
                "ctl00$updpAreaConsulta|ctl00$cphConsulta$butVisualizarResultado"
            ),
            "__EVENTTARGET": "",
            "__VIEWSTATE": states["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
            "ctl00$cphConsulta$butVisualizarResultado": "Visualizar e salvar",
        }
        if states.get("__EVENTVALIDATION"):
            show["__EVENTVALIDATION"] = states["__EVENTVALIDATION"]
        self._post(page, data=show, headers=ajax)

    # 5. download
    def download_csv(self) -> requests.Response:
        legacy = self.query.legacy_url
        self._get(legacy + "visualizaconsulta.aspx", headers={"Referer": legacy})
        frame_url = legacy + "VisualizaConsultaFrame.aspx"
        frame = self._get(frame_url, headers={"Referer": legacy})
        if self.query.series not in frame.text:
            raise self._fail("o quadro da consulta não trouxe a série")
        states = _states(frame.text)
        form = {
            "__EVENTTARGET": "lbtSalvarCSV",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": states["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
        }
        if states.get("__EVENTVALIDATION"):
            form["__EVENTVALIDATION"] = states["__EVENTVALIDATION"]
        for field in ("xgdvConsulta", "DXScript", "DXCss"):
            match = re.search(rf'name="{field}"[^>]*value="(.*?)"', frame.text)
            if match:
                form[field] = html.unescape(match.group(1))
        response = self._post(
            frame_url,
            data=form,
            headers={"Referer": frame_url},
            stream=True,
        )
        content_type = response.headers.get("Content-Type", "").lower()
        if not response.ok or not content_type.startswith(CSV_TYPES):
            response.close()
            raise self._fail(
                f"download não devolveu CSV (HTTP {response.status_code}, "
                f"{content_type or 'sem Content-Type'})"
            )
        return response


def _login_payload(
    versions: dict[str, str], api_version: str, email: str, password: str
) -> dict[str, Any]:
    """Corpo das DataActions da tela de login (a do CloudFlare vai sem credencial)."""
    return {
        "versionInfo": {
            "moduleVersion": versions["moduleVersion"],
            "apiVersion": api_version,
        },
        "viewName": "MainFlow.Login",
        "screenData": {
            "variables": {
                "DSLogin": email,
                "DSPassword": password,
                "Prompt_Login": "",
                "Prompt_Senha": "",
                "MSG_Erro": "",
                "MSG_AtivacaoCadastro": "",
                "MSG_ErroCodigo": "",
                "LoadingBotaoEntrar": bool(email),
                "LoadingBotaoValidar": False,
                "FLG_AtivacaoCadastro": False,
                "FLG_ExibirSenha": False,
                "FLG_PopupAtivarConta": False,
                "FLG_VerificarCodigo": True,
                "WidgetIdFlare": "b4-b4-CfTurnstile" if email else "",
                "VL_TentativasLogin": 0,
                "CD_01": "",
                "CD_02": "",
                "CD_03": "",
                "CD_04": "",
                "FLG_ativarConta": False,
                "_fLG_ativarContaInDataFetchStatus": 1,
                "token": "",
                "_tokenInDataFetchStatus": 1,
                "Email": "",
                "_emailInDataFetchStatus": 1,
            }
        },
        "clientVariables": {"RL_Produtos": "", "NM_Usuario": ""},
    }


def _ajax_headers(referer: str) -> dict[str, str]:
    origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(referer))
    return {
        "X-Requested-With": "XMLHttpRequest",
        "X-MicrosoftAjax": "Delta=true",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Cache-Control": "no-cache",
        "Origin": origin,
        "Referer": referer,
    }


def _states(page: str) -> dict[str, str]:
    """Campos ocultos do ASP.NET WebForms numa página inteira."""
    states = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        match = re.search(rf'id="{name}"\s+value="(.*?)"', page)
        states[name] = match.group(1) if match else ""
    return states


def _delta_states(delta: str) -> dict[str, str]:
    """Campos ocultos numa resposta parcial do UpdatePanel (`n|tipo|id|valor|`)."""
    wanted = ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")
    parts = delta.split("|")
    states: dict[str, str] = {}
    i = 0
    while i + 3 < len(parts):
        if not parts[i].isdigit():
            i += 1
            continue
        if parts[i + 1] == "hiddenField" and parts[i + 2] in wanted:
            states[parts[i + 2]] = parts[i + 3]
        i += 4
    return states
