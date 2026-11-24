import logging
import urllib.parse
import re
import html
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context


class LegacySSLAdapter(HTTPAdapter):
    """
    Adaptador customizado para baixar o nível de segurança do OpenSSL
    e permitir conexão com servidores IIS legados (ASP.NET).
    """

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        context = create_urllib3_context()
        context.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = context
        return super(LegacySSLAdapter, self).init_poolmanager(*args, **kwargs)


class FGVDadosClient:
    """
    Cliente para extração autenticada de dados históricos (ICST) do portal FGVDados.
    Lida com a autenticação OutSystems e a navegação no sistema legado ASP.NET.
    """

    def __init__(
        self,
        email: str,
        password: str,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.email = email
        self.password = password
        # A DAG passa a sessão do HttpHook (Connection fgv_dados); o SSL legado vale só
        # para o ASP.NET, não para o login do OutSystems.
        self.session = session or requests.Session()
        self.session.mount("https://extra-ibre.fgv.br", LegacySSLAdapter())
        logging.info("[fgv_client] Initialized FGVDadosClient")

    def _outsystems_versions(self) -> dict:
        """
        Extrai dinamicamente moduleVersion, apiVersions e
        URLs dos endpoints de login do OutSystems.

        Estratégia:
        1. moduleVersion vem do endpoint REST
           /moduleservices/moduleversioninfo
        2. apiVersions e URLs vêm do JS lazy-loaded
           BL01_Login.mvc.js, onde o OutSystems registra
           chamadas no formato:
           callDataAction("...", "screenservices/...", "HASH", ...)
        """
        logging.info("[fgv_client] Buscando hashes dinâmicas do OutSystems...")
        url_base = "https://autenticacao-ibre.fgv.br/ProdutosDigitais/"

        # Fallbacks caso algo falhe
        result = {
            "moduleVersion": "vuthrRMgPWqGaqAin6KHTA",
            "api_cloudflare": "OTGxLOOIYRPT4Yzxi6zCuA",
            "api_login": "kEIaQNU5n93i9Q026f_dlQ",
            "url_cloudflare": (
                "screenservices/ProdutosDigitais/Blocks/BL01_Login/DataActionCheckUsarCloudFlare"
            ),
            "url_login": (
                "screenservices/ProdutosDigitais/Blocks/BL01_Login/DataActionGetDadosLogin"
            ),
        }

        # Session pode ter Accept: application/json,
        # o que causa 406 ao baixar .js
        headers_get = {"Accept": "*/*"}

        # moduleVersion via REST
        try:
            res_ver = self.session.get(
                url_base + "moduleservices/moduleversioninfo",
                timeout=10,
                headers=headers_get,
            )
            if res_ver.status_code == 200:
                token = res_ver.json().get("versionToken", "")
                if token:
                    result["moduleVersion"] = token
        except Exception as e:
            logging.warning(f"[fgv_client] Falha ao buscar moduleversioninfo: {e}")

        # apiVersions e URLs do BL01_Login.mvc.js
        try:
            url_js = url_base + "scripts/ProdutosDigitais.Blocks.BL01_Login.mvc.js"
            res_js = self.session.get(url_js, timeout=10, headers=headers_get)
            if res_js.status_code == 200:
                js = res_js.text

                # Sufixo do DataAction -> chaves no result
                actions = {
                    "DataActionCheckUsarCloudFlare": (
                        "api_cloudflare",
                        "url_cloudflare",
                    ),
                    "DataActionGET_UsarCloudFlare": (
                        "api_cloudflare",
                        "url_cloudflare",
                    ),
                    "DataActionGetDadosLogin": (
                        "api_login",
                        "url_login",
                    ),
                    "DataActionGET_Dados": (
                        "api_login",
                        "url_login",
                    ),
                }

                regex_call = (
                    r'callDataAction\(\s*"[^"]*"\s*,\s*'
                    r'"(screenservices/[^"]*'
                    r'/(DataAction\w+))"\s*,\s*'
                    r'"([^"]+)"'
                )
                for m in re.finditer(regex_call, js):
                    url_path = m.group(1)
                    action_name = m.group(2)
                    api_hash = m.group(3)
                    if action_name in actions:
                        k_hash, k_url = actions[action_name]
                        result[k_hash] = api_hash
                        result[k_url] = url_path
        except Exception as e:
            logging.warning(f"[fgv_client] Falha ao buscar BL01_Login.mvc.js: {e}")

        logging.info(
            "[fgv_client] Hashes:"
            f" Module={result['moduleVersion'][:8]}..."
            f" | CF={result['api_cloudflare'][:8]}..."
            f" | Login={result['api_login'][:8]}..."
        )
        return result

    @staticmethod
    def _extract_states(html_text: str) -> dict:
        """Extrai campos ocultos exigidos pela arquitetura ASP.NET WebForms."""
        vs = re.search(r'id="__VIEWSTATE"\s+value="(.*?)"', html_text)
        vsg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="(.*?)"', html_text)
        ev = re.search(r'id="__EVENTVALIDATION"\s+value="(.*?)"', html_text)
        return {
            "__VIEWSTATE": vs.group(1) if vs else "",
            "__VIEWSTATEGENERATOR": vsg.group(1) if vsg else "",
            "__EVENTVALIDATION": ev.group(1) if ev else "",
        }

    @staticmethod
    def _extract_delta_states(delta_text: str) -> dict:
        """Extrai campos ocultos de respostas parciais (AJAX) do UpdatePanel."""
        states = {}
        parts = delta_text.split("|")
        i = 0
        while i + 3 < len(parts):
            try:
                int(parts[i])
            except ValueError:
                i += 1
                continue

            is_hidden = parts[i + 1] == "hiddenField"
            valid_fields = (
                "__VIEWSTATE",
                "__VIEWSTATEGENERATOR",
                "__EVENTVALIDATION",
            )

            if is_hidden and parts[i + 2] in valid_fields:
                states[parts[i + 2]] = parts[i + 3]
            i += 4
        return states

    def _authenticate(self, dynamic_hashes: dict) -> Optional[str]:
        """
        Executa o fluxo de login no portal OutSystems e prepara a sessão
        para transição aos sistemas ASP.NET legados.
        """
        logging.info("[fgv_client] Inicializando autenticação FGV...")
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) Gecko/20100101 Firefox/149.0"
                ),
                "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json",
                "OutSystems-Client-Type": "Web",
                "Origin": "https://autenticacao-ibre.fgv.br",
                "Referer": "https://autenticacao-ibre.fgv.br/ProdutosDigitais/",
            }
        )

        # Autenticação OutSystems
        self.session.get("https://autenticacao-ibre.fgv.br/ProdutosDigitais/")
        self.session.headers.update({"X-CSRFToken": ""})

        url_cf = (
            "https://autenticacao-ibre.fgv.br/ProdutosDigitais/" + dynamic_hashes["url_cloudflare"]
        )

        payload_cf = {
            "versionInfo": {
                "moduleVersion": dynamic_hashes["moduleVersion"],
                "apiVersion": dynamic_hashes["api_cloudflare"],
            },
            "viewName": "MainFlow.Login",
            "screenData": {
                "variables": {
                    "DSLogin": "",
                    "DSPassword": "",
                    "Prompt_Login": "ex: seu login ou email@email.com",
                    "Prompt_Senha": "digite a tua senha",
                    "MSG_Erro": "",
                    "MSG_AtivacaoCadastro": "",
                    "MSG_ErroCodigo": "",
                    "LoadingBotaoEntrar": False,
                    "LoadingBotaoValidar": False,
                    "FLG_AtivacaoCadastro": False,
                    "FLG_ExibirSenha": False,
                    "FLG_PopupAtivarConta": False,
                    "FLG_VerificarCodigo": True,
                    "WidgetIdFlare": "",
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

        self.session.post(url_cf, json=payload_cf)

        cookie_nr2users = self.session.cookies.get("nr2Users")
        if not cookie_nr2users:
            logging.error("[fgv_client] Falha ao capturar token (nr2Users).")
            return None

        token_unquoted = urllib.parse.unquote(cookie_nr2users)
        token_csrf = token_unquoted.split("crf=")[1].split(";")[0]
        self.session.headers.update({"X-CSRFToken": token_csrf})

        url_login = (
            "https://autenticacao-ibre.fgv.br/ProdutosDigitais/" + dynamic_hashes["url_login"]
        )

        payload_login = {
            "versionInfo": {
                "moduleVersion": dynamic_hashes["moduleVersion"],
                "apiVersion": dynamic_hashes["api_login"],
            },
            "viewName": "MainFlow.Login",
            "screenData": {
                "variables": {
                    "DSLogin": self.email,
                    "DSPassword": self.password,
                    "Prompt_Login": "",
                    "Prompt_Senha": "",
                    "MSG_Erro": "",
                    "MSG_AtivacaoCadastro": "",
                    "MSG_ErroCodigo": "",
                    "LoadingBotaoEntrar": True,
                    "LoadingBotaoValidar": False,
                    "FLG_AtivacaoCadastro": False,
                    "FLG_ExibirSenha": False,
                    "FLG_PopupAtivarConta": False,
                    "FLG_VerificarCodigo": True,
                    "WidgetIdFlare": "b4-b4-CfTurnstile",
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

        res_login = self.session.post(url_login, json=payload_login)
        response_data = res_login.json()

        if not response_data.get("data", {}).get("FLG_Sucesso"):
            logging.error("[fgv_client] Credenciais rejeitadas/login falhou.")
            return None

        url_redir: str = response_data["data"]["URL_Gratuito"]
        logging.info("[fgv_client] Acesso autorizado. Migrando para ASP.NET.")

        headers_to_remove = [
            "Content-Type",
            "Accept",
            "OutSystems-Client-Type",
            "Origin",
            "Referer",
            "X-CSRFToken",
        ]

        # Limpeza de headers modernos para transição
        for h in headers_to_remove:
            self.session.headers.pop(h, None)

        return url_redir

    def _search_icst_series(self, url_redir: str) -> bool:
        """
        Acessa a página inicial do sistema legado, busca a série 'ICST'
        e a seleciona contornando a validação de eventos do ASP.NET.
        """

        res_default = self.session.get(url_redir)
        states = self._extract_states(res_default.text)
        url_base_default = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/Default.aspx"

        headers_ajax = {
            "X-Requested-With": "XMLHttpRequest",
            "X-MicrosoftAjax": "Delta=true",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Cache-Control": "no-cache",
            "Origin": "https://extra-ibre.fgv.br",
            "Referer": url_base_default,
        }

        # Fluxo de Busca e Seleção (Bypass Event Validation)
        logging.info("[fgv_client] Executando busca paramétrica...")
        res_search = self.session.post(
            url_base_default,
            data={
                "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeries",
                "ctl00$drpFiltro": "E",
                "ctl00$txtBuscarSeries": "ICST",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": states["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
                "ctl00$butBuscarSeries": "OK",
            },
            headers=headers_ajax,
        )

        delta_states = self._extract_delta_states(res_search.text)
        if delta_states.get("__VIEWSTATE"):
            states.update(delta_states)

        if "ICST" not in res_search.text:
            logging.error("[fgv_client] Série ICST não encontrada na busca.")
            return False

        self.session.post(
            url_base_default,
            data={
                "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeriesOK",
                "ctl00$drpFiltro": "E",
                "ctl00$txtBuscarSeries": "ICST",
                "ctl00$dlsSerie$ctl00$chkSerieEscolhida": "on",
                "ctl00$dlsSerie$ctl01$chkSerieEscolhida": "on",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": states["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
                "ctl00$butBuscarSeriesOK": "OK",
            },
            headers=headers_ajax,
        )

        return True

    def _configure_query(self) -> bool:
        """
        Navega para a página de consulta, seleciona a opção de série histórica
        e submete o formulário para preparar o grid de visualização.
        """

        # Configuração da Visualização
        logging.info("[fgv_client] Configurando parâmetros históricos...")
        url_query = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/consulta.aspx"
        res_consulta = self.session.get(url_query)
        states = self._extract_states(res_consulta.text)

        if "ICST" not in res_consulta.text:
            logging.error("[fgv_client] Séries não persistidas na sessão.")
            return False

        headers_ajax_query = {
            "X-Requested-With": "XMLHttpRequest",
            "X-MicrosoftAjax": "Delta=true",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "Cache-Control": "no-cache",
            "Origin": "https://extra-ibre.fgv.br",
            "Referer": url_query,
        }

        res_radio = self.session.post(
            url_query,
            data={
                "ctl00$smg": ("ctl00$cphConsulta$updpOpcoes|ctl00$cphConsulta$rbtSerieHistorica"),
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
                "__EVENTTARGET": "ctl00$cphConsulta$rbtSerieHistorica",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": states["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
            },
            headers=headers_ajax_query,
        )

        delta_states = self._extract_delta_states(res_radio.text)
        if delta_states.get("__VIEWSTATE"):
            states.update(delta_states)

        payload_viz = {
            "ctl00$smg": ("ctl00$updpAreaConsulta|ctl00$cphConsulta$butVisualizarResultado"),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": states["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": states["__VIEWSTATEGENERATOR"],
            "__VIEWSTATEENCRYPTED": "",
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
            "__ASYNCPOST": "true",
            "ctl00$cphConsulta$butVisualizarResultado": "Visualizar e salvar",
        }
        if states.get("__EVENTVALIDATION"):
            payload_viz["__EVENTVALIDATION"] = states["__EVENTVALIDATION"]

        self.session.post(url_query, data=payload_viz, headers=headers_ajax_query)

        return True

    def _download_csv(self) -> Optional[bytes]:
        """
        Acessa o frame de visualização, extrai as variáveis de estado do
        DevExpress e emite a requisição final de download do CSV (bytes originais).
        """
        url_query = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/consulta.aspx"
        url_view = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/visualizaconsulta.aspx"

        # Preparação do grid de download
        self.session.get(url_view, headers={"Referer": url_query})

        url_frame = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/VisualizaConsultaFrame.aspx"
        res_frame = self.session.get(url_frame, headers={"Referer": url_view})

        if "ICST" not in res_frame.text:
            logging.error("[fgv_client] Componentes DevExpress falharam.")
            return None

        frame_states = self._extract_states(res_frame.text)

        payload_download = {
            "__EVENTTARGET": "lbtSalvarCSV",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": frame_states["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": frame_states["__VIEWSTATEGENERATOR"],
        }
        if frame_states.get("__EVENTVALIDATION"):
            payload_download["__EVENTVALIDATION"] = frame_states["__EVENTVALIDATION"]

        # Campos opcionais do DevExpress grid
        regexes_dx = [
            ("xgdvConsulta", r'name="xgdvConsulta"[^>]*value="(.*?)"'),
            ("DXScript", r'name="DXScript"[^>]*value="(.*?)"'),
            ("DXCss", r'name="DXCss"[^>]*value="(.*?)"'),
        ]

        for field, regex in regexes_dx:
            match = re.search(regex, res_frame.text)
            if match:
                payload_download[field] = html.unescape(match.group(1))

        # Download do CSV
        logging.info("[fgv_client] Emitindo solicitação final de extração...")
        res_csv = self.session.post(
            url_frame,
            data=payload_download,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
                "Origin": "https://extra-ibre.fgv.br",
                "Referer": url_frame,
            },
        )

        content_type = res_csv.headers.get("Content-Type", "").lower()
        is_valid_type = "text/csv" in content_type or "application/octet-stream" in content_type

        if res_csv.status_code == 200 and is_valid_type:
            return res_csv.content
        logging.error(
            f"[fgv_client] Falha na extração. HTTP {res_csv.status_code} | Type: {content_type}"
        )
        return None

    def download_icst_csv(self) -> Optional[bytes]:
        """
        Orquestrador do fluxo completo de autenticação e raspagem do CSV
        da série histórica (ICST).

        Returns:
            Os bytes do CSV como a FGV entrega, ou None em falha.
        """
        dynamic_hashes = self._outsystems_versions()

        url_redir = self._authenticate(dynamic_hashes)
        if not url_redir:
            return None

        if not self._search_icst_series(url_redir):
            return None

        if not self._configure_query():
            return None

        return self._download_csv()
