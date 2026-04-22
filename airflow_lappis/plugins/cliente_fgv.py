import logging
import io
import urllib.parse
import re
import html
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context


class LegacySSLAdapter(HTTPAdapter):
    """
    Adaptador customizado para baixar o nível de segurança do OpenSSL
    e permitir conexão com servidores IIS legados (ASP.NET).
    """

    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = context
        return super(LegacySSLAdapter, self).init_poolmanager(*args, **kwargs)


class ClienteSinduscon:
    """
    Cliente para extração de dados INCC-M da FGV no site da Sinduscon.

    Fonte: https://sindusconpr.com.br/download/10984/1364
    """

    URL_INCC = "https://sindusconpr.com.br/download/10984/1364"

    def __init__(self) -> None:
        logging.info("[cliente_fgv.py] Initialized ClienteFGV")

    def fetch_and_transform_incc(self) -> Optional[list[dict]]:
        """
        Baixa o arquivo XLSX do INCC-M, limpa os cabeçalhos e formata os dados.

        Returns:
            Lista de dicionários contendo os registros do índice ou None em caso de falha.
        """
        logging.info(f"[cliente_fgv.py] Baixando dados de: {self.URL_INCC}")

        # User-Agent previne que alguns servidores bloqueiem
        # requisições de bibliotecas python
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " "AppleWebKit/537.36"
            )
        }

        try:
            response = requests.get(self.URL_INCC, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"[cliente_fgv.py] Erro ao baixar o arquivo da FGV: {e}")
            return None

        try:
            # Lendo o Excel em memória
            # Pula as 3 primeiras linhas de cabeçalho
            df = pd.read_excel(
                io.BytesIO(response.content),
                skiprows=3,
                usecols="A:E",
                names=["mes", "indice", "var_mes", "var_ano", "var_12_meses"],
            )

            # Remove as linhas vazias ou o rodapé de "Fonte FGV"
            df = df.dropna(subset=["mes"])
            df = df[~df["mes"].astype(str).str.contains("Fonte", case=False, na=False)]

            # # Adiciona data de ingestão
            df["dt_ingest"] = datetime.now().isoformat()

            # Converte valores pd.NA/NaN para None (NULL do Postgres)
            df = df.where(pd.notnull(df), None)

            registros = df.to_dict(orient="records")

            logging.info(
                f"[cliente_fgv.py] Transformação concluída. "
                f"{len(registros)} registros preparados."
            )

            return registros

        except Exception as e:
            logging.error(f"[cliente_fgv.py] Erro ao processar arquivo com Pandas: {e}")
            return None


class ClienteFGVDados:
    """
    Cliente para extração autenticada de dados históricos (ICST) do portal FGVDados.
    Lida com a autenticação OutSystems e a navegação no sistema legado ASP.NET.
    """

    def __init__(
        self,
        email: str,
        password: str,
    ) -> None:
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.session.mount("https://extra-ibre.fgv.br", LegacySSLAdapter())
        logging.info("[cliente_fgv.py] Initialized ClienteFGVDados")

    def _obter_versoes_outsystems(self) -> dict:
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
        logging.info(
            "[cliente_fgv.py] Buscando hashes"
            " dinâmicas do OutSystems..."
        )
        url_base = (
            "https://autenticacao-ibre.fgv.br"
            "/ProdutosDigitais/"
        )

        # Fallbacks caso algo falhe
        resultado = {
            "moduleVersion": "vuthrRMgPWqGaqAin6KHTA",
            "api_cloudflare": "OTGxLOOIYRPT4Yzxi6zCuA",
            "api_login": "kEIaQNU5n93i9Q026f_dlQ",
            "url_cloudflare": (
                "screenservices/ProdutosDigitais/"
                "Blocks/BL01_Login/"
                "DataActionCheckUsarCloudFlare"
            ),
            "url_login": (
                "screenservices/ProdutosDigitais/"
                "Blocks/BL01_Login/"
                "DataActionGetDadosLogin"
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
                    resultado["moduleVersion"] = token
        except Exception as e:
            logging.warning(
                "[cliente_fgv.py] Falha ao buscar"
                f" moduleversioninfo: {e}"
            )

        # apiVersions e URLs do BL01_Login.mvc.js
        try:
            url_js = (
                url_base
                + "scripts/ProdutosDigitais"
                ".Blocks.BL01_Login.mvc.js"
            )
            res_js = self.session.get(
                url_js, timeout=10, headers=headers_get
            )
            if res_js.status_code == 200:
                js = res_js.text

                # Sufixo do DataAction -> chaves no resultado
                acoes = {
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
                    if action_name in acoes:
                        k_hash, k_url = acoes[action_name]
                        resultado[k_hash] = api_hash
                        resultado[k_url] = url_path
        except Exception as e:
            logging.warning(
                "[cliente_fgv.py] Falha ao buscar"
                f" BL01_Login.mvc.js: {e}"
            )

        logging.info(
            "[cliente_fgv.py] Hashes:"
            f" Module={resultado['moduleVersion'][:8]}..."
            f" | CF={resultado['api_cloudflare'][:8]}..."
            f" | Login={resultado['api_login'][:8]}..."
        )
        return resultado

    @staticmethod
    def _extrair_estados(html_text: str) -> dict:
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
    def _extrair_estados_delta(delta_text: str) -> dict:
        """Extrai campos ocultos de respostas parciais (AJAX) do UpdatePanel."""
        estados = {}
        partes = delta_text.split("|")
        i = 0
        while i + 3 < len(partes):
            try:
                int(partes[i])
            except ValueError:
                i += 1
                continue

            eh_hidden = partes[i + 1] == "hiddenField"
            campos_validos = (
                "__VIEWSTATE",
                "__VIEWSTATEGENERATOR",
                "__EVENTVALIDATION",
            )

            if eh_hidden and partes[i + 2] in campos_validos:
                estados[partes[i + 2]] = partes[i + 3]
            i += 4
        return estados

    def fetch_icst_historico(self) -> Optional[list[dict]]:  # noqa: C901
        """
        Fluxo completo de autenticação e raspagem do CSV da série histórica (ICST).

        Returns:
            DataFrame Pandas contendo dados brutos em sucesso, ou None em falha.
        """
        hashes_dinamicas = self._obter_versoes_outsystems()

        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) "
                    "Gecko/20100101 Firefox/149.0"
                ),
                "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json",
                "OutSystems-Client-Type": "Web",
                "Origin": "https://autenticacao-ibre.fgv.br",
                "Referer": "https://autenticacao-ibre.fgv.br/ProdutosDigitais/",
            }
        )

        # Autenticação OutSystems
        logging.info("[cliente_fgv.py] Inicializando autenticação FGV...")
        self.session.get("https://autenticacao-ibre.fgv.br/ProdutosDigitais/")
        self.session.headers.update({"X-CSRFToken": ""})

        url_cf = (
            "https://autenticacao-ibre.fgv.br/ProdutosDigitais/"
            + hashes_dinamicas["url_cloudflare"]
        )

        payload_cf = {
            "versionInfo": {
                "moduleVersion": hashes_dinamicas["moduleVersion"],
                "apiVersion": hashes_dinamicas["api_cloudflare"],
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
            logging.error("[cliente_fgv.py] Falha ao capturar token (nr2Users).")
            return None

        token_unquoted = urllib.parse.unquote(cookie_nr2users)
        token_csrf = token_unquoted.split("crf=")[1].split(";")[0]
        self.session.headers.update({"X-CSRFToken": token_csrf})

        url_login = (
            "https://autenticacao-ibre.fgv.br/ProdutosDigitais/"
            + hashes_dinamicas["url_login"]
        )

        payload_login = {
            "versionInfo": {
                "moduleVersion": hashes_dinamicas["moduleVersion"],
                "apiVersion": hashes_dinamicas["api_login"],
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
        dados_resposta = res_login.json()

        if not dados_resposta.get("data", {}).get("FLG_Sucesso"):
            logging.error("[cliente_fgv.py] Credenciais rejeitadas/login falhou.")
            return None

        url_redir = dados_resposta["data"]["URL_Gratuito"]
        logging.info("[cliente_fgv.py] Acesso autorizado. Migrando para ASP.NET.")

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

        res_default = self.session.get(url_redir)
        estados = self._extrair_estados(res_default.text)
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
        logging.info("[cliente_fgv.py] Executando busca paramétrica...")
        res_search = self.session.post(
            url_base_default,
            data={
                "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeries",
                "ctl00$drpFiltro": "E",
                "ctl00$txtBuscarSeries": "ICST",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": estados["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": estados["__VIEWSTATEGENERATOR"],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
                "ctl00$butBuscarSeries": "OK",
            },
            headers=headers_ajax,
        )

        delta_estados = self._extrair_estados_delta(res_search.text)
        if delta_estados.get("__VIEWSTATE"):
            estados.update(delta_estados)

        if "ICST" not in res_search.text:
            logging.error("[cliente_fgv.py] Série ICST não encontrada na busca.")
            return None

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
                "__VIEWSTATE": estados["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": estados["__VIEWSTATEGENERATOR"],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
                "ctl00$butBuscarSeriesOK": "OK",
            },
            headers=headers_ajax,
        )

        # Configuração da Visualização
        logging.info("[cliente_fgv.py] Configurando parâmetros históricos...")
        url_consulta = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/consulta.aspx"
        res_consulta = self.session.get(url_consulta)
        estados = self._extrair_estados(res_consulta.text)

        if "ICST" not in res_consulta.text:
            logging.error("[cliente_fgv.py] Séries não persistidas na sessão.")
            return None

        headers_ajax_consulta = {**headers_ajax, "Referer": url_consulta}

        res_radio = self.session.post(
            url_consulta,
            data={
                "ctl00$smg": (
                    "ctl00$cphConsulta$updpOpcoes|" "ctl00$cphConsulta$rbtSerieHistorica"
                ),
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
                "__VIEWSTATE": estados["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": estados["__VIEWSTATEGENERATOR"],
                "__VIEWSTATEENCRYPTED": "",
                "__ASYNCPOST": "true",
            },
            headers=headers_ajax_consulta,
        )

        delta_estados = self._extrair_estados_delta(res_radio.text)
        if delta_estados.get("__VIEWSTATE"):
            estados.update(delta_estados)

        payload_viz = {
            "ctl00$smg": (
                "ctl00$updpAreaConsulta|" "ctl00$cphConsulta$butVisualizarResultado"
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": estados["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": estados["__VIEWSTATEGENERATOR"],
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
        if estados.get("__EVENTVALIDATION"):
            payload_viz["__EVENTVALIDATION"] = estados["__EVENTVALIDATION"]

        self.session.post(url_consulta, data=payload_viz, headers=headers_ajax_consulta)

        url_visualiza = (
            "https://extra-ibre.fgv.br/IBRE/sitefgvdados/visualizaconsulta.aspx"
        )

        # Preparação do grid de download
        self.session.get(url_visualiza, headers={"Referer": url_consulta})

        url_frame = (
            "https://extra-ibre.fgv.br/IBRE/sitefgvdados/" "VisualizaConsultaFrame.aspx"
        )
        res_frame = self.session.get(url_frame, headers={"Referer": url_visualiza})

        if "ICST" not in res_frame.text:
            logging.error("[cliente_fgv.py] Componentes DevExpress falharam.")
            return None

        estados_frame = self._extrair_estados(res_frame.text)

        payload_download = {
            "__EVENTTARGET": "lbtSalvarCSV",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": estados_frame["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": estados_frame["__VIEWSTATEGENERATOR"],
        }
        if estados_frame.get("__EVENTVALIDATION"):
            payload_download["__EVENTVALIDATION"] = estados_frame["__EVENTVALIDATION"]

        # Campos opcionais do DevExpress grid
        regexes_dx = [
            ("xgdvConsulta", r'name="xgdvConsulta"[^>]*value="(.*?)"'),
            ("DXScript", r'name="DXScript"[^>]*value="(.*?)"'),
            ("DXCss", r'name="DXCss"[^>]*value="(.*?)"'),
        ]

        for campo, regex in regexes_dx:
            match = re.search(regex, res_frame.text)
            if match:
                payload_download[campo] = html.unescape(match.group(1))

        # Download do CSV
        logging.info("[cliente_fgv.py] Emitindo solicitação final de extração...")
        res_csv = self.session.post(
            url_frame,
            data=payload_download,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;" "q=0.9,*/*;q=0.8"
                ),
                "Origin": "https://extra-ibre.fgv.br",
                "Referer": url_frame,
            },
        )

        content_type = res_csv.headers.get("Content-Type", "").lower()
        is_valid_type = (
            "text/csv" in content_type or "application/octet-stream" in content_type
        )

        if res_csv.status_code == 200 and is_valid_type:
            try:
                df = pd.read_csv(
                    io.BytesIO(res_csv.content),
                    sep=";",
                    encoding="latin1",
                    header=0,
                    names=["mes", "icst_com_ajuste_sazonal", "icst_sem_ajuste_sazonal"],
                )

                # Adiciona data de ingestão
                df["dt_ingest"] = datetime.now().isoformat()

                # Converte valores pd.NA/NaN para None (NULL do Postgres)
                df = df.where(pd.notnull(df), None)

                registros = df.to_dict(orient="records")

                logging.info(
                    f"[cliente_fgv.py] Extração concluída. "
                    f"DataFrame construído com {len(df)} registros."
                )
                return registros
            except Exception as e:
                logging.error(f"[cliente_fgv.py] Falha no parser do Pandas: {e}")
                return None
        else:
            logging.error(
                f"[cliente_fgv.py] Falha na extração. "
                f"HTTP {res_csv.status_code} | Type: {content_type}"
            )
            return None

