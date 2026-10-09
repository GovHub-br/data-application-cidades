"""ICST (FGV-IBRE): Índice de Confiança da Construção, com e sem ajuste sazonal.

Fonte: portal FGVDados, sem API. O CSV da série histórica só sai depois do login
no OutSystems (`autenticacao-ibre`) e de navegar no FGVDados (`extra-ibre`,
ASP.NET WebForms), numa sessão só: a estratégia genérica `http_session` executa
o fluxo declarado aqui (`fluxo`). As credenciais vêm das Variables
`dados_fgv_email` e `dados_fgv_password`, lidas só dentro da task.

O fluxo, como um navegador faria:

1. versões do OutSystems (mudam a cada deploy do portal; sem elas, valem as do
   último deploy conhecido);
2. login: token CSRF do cookie `nr2Users`, credenciais, URL de entrada no
   FGVDados;
3. busca e seleção da série, com os campos ocultos do WebForms (`__VIEWSTATE`…),
   que a resposta parcial do UpdatePanel atualiza (`n|hiddenField|nome|valor|`);
4. consulta da série histórica;
5. download do CSV (latin-1, `;`), gravado como veio.

LoadMode: overwrite. O CSV traz a série inteira desde 07/2010 a cada download,
então a última ingestão é a verdade e revisões da FGV entram.

Estrutura do arquivo: cabeçalho na linha 1 com o nome longo de cada série e o
código da FGV entre parênteses; valores com vírgula decimal. Renomear e tipar é da
prata (`prata_conjuntura_fgv_icst`).
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig
from ingestion.extractors.models.http_common import LegacyTlsAdapter
from ingestion.extractors.session import (
    Contains,
    Cookie,
    Download,
    DropHeaders,
    HttpSession,
    JsonEquals,
    JsonField,
    Regex,
    Request,
    SetHeaders,
)
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

AUTH = "https://autenticacao-ibre.fgv.br/ProdutosDigitais/"
LEGACY = "https://extra-ibre.fgv.br/IBRE/sitefgvdados/"
SERIE = "ICST"
LOGIN_SCREEN = "screenservices/ProdutosDigitais/Blocks/BL01_Login/"
CALL_DATA_ACTION = (
    r'callDataAction\(\s*"[^"]*"\s*,\s*"(screenservices/[^"]*/DataAction(?:{acoes}))"'
    r'\s*,\s*"([^"]+)"'
)
CLOUDFLARE = CALL_DATA_ACTION.format(acoes="CheckUsarCloudFlare|GET_UsarCloudFlare")
DADOS_LOGIN = CALL_DATA_ACTION.format(acoes="GetDadosLogin|GET_Dados")


def _campos_ocultos(parcial: bool) -> dict[str, Regex]:
    """Campos ocultos do WebForms: na página inteira ou na resposta parcial."""
    campos = {
        "viewstate": "__VIEWSTATE",
        "viewstategenerator": "__VIEWSTATEGENERATOR",
        "eventvalidation": "__EVENTVALIDATION",
    }
    if parcial:  # resposta do UpdatePanel: só atualiza o que veio
        return {
            nome: Regex(rf"\|hiddenField\|{campo}\|([^|]*)\|", required=False)
            for nome, campo in campos.items()
        }
    return {
        nome: Regex(rf'id="{campo}"\s+value="(.*?)"', default="")
        for nome, campo in campos.items()
    }


def _login(api: str, email: str, senha: str) -> dict[str, Any]:
    """Corpo das DataActions da tela de login do OutSystems."""
    com_credencial = bool(email)
    return {
        "versionInfo": {"moduleVersion": "{module_version}", "apiVersion": api},
        "viewName": "MainFlow.Login",
        "screenData": {
            "variables": {
                "DSLogin": email,
                "DSPassword": senha,
                "Prompt_Login": "",
                "Prompt_Senha": "",
                "MSG_Erro": "",
                "MSG_AtivacaoCadastro": "",
                "MSG_ErroCodigo": "",
                "LoadingBotaoEntrar": com_credencial,
                "LoadingBotaoValidar": False,
                "FLG_AtivacaoCadastro": False,
                "FLG_ExibirSenha": False,
                "FLG_PopupAtivarConta": False,
                "FLG_VerificarCodigo": True,
                "WidgetIdFlare": "b4-b4-CfTurnstile" if com_credencial else "",
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


def _ajax(referer: str, origem: str) -> dict[str, str]:
    return {
        "X-Requested-With": "XMLHttpRequest",
        "X-MicrosoftAjax": "Delta=true",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Cache-Control": "no-cache",
        "Origin": origem,
        "Referer": referer,
    }


def _form_consulta() -> dict[str, str]:
    return {
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
        "__VIEWSTATE": "{viewstate}",
        "__VIEWSTATEGENERATOR": "{viewstategenerator}",
        "__VIEWSTATEENCRYPTED": "",
        "__ASYNCPOST": "true",
    }


def fluxo(auth: str = AUTH, legacy: str = LEGACY) -> HttpSession:
    """O caminho do navegador até o CSV; as URLs base mudam só nos testes."""
    origem_auth = auth.split("/ProdutosDigitais/")[0]
    origem_legacy = legacy.split("/IBRE/")[0]
    pagina, consulta = legacy + "Default.aspx", legacy + "consulta.aspx"
    quadro = legacy + "VisualizaConsultaFrame.aspx"
    busca = {
        "ctl00$drpFiltro": "E",
        "ctl00$txtBuscarSeries": SERIE,
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": "{viewstate}",
        "__VIEWSTATEGENERATOR": "{viewstategenerator}",
        "__VIEWSTATEENCRYPTED": "",
        "__ASYNCPOST": "true",
    }
    return HttpSession(
        variables={"email": "dados_fgv_email", "senha": "dados_fgv_password"},
        # O IIS legado do FGVDados só negocia TLS com nível de segurança 1.
        mounts={origem_legacy: LegacyTlsAdapter()},
        steps=(
            # 1. versões do OutSystems (com Accept: application/json o .js dá 406)
            Request(
                "versao_do_modulo",
                "GET",
                auth + "moduleservices/moduleversioninfo",
                headers={"Accept": "*/*"},
                check_status=False,
                capture={
                    "module_version": JsonField(
                        "versionToken", default="vuthrRMgPWqGaqAin6KHTA"
                    )
                },
            ),
            Request(
                "versoes_do_login",
                "GET",
                auth + "scripts/ProdutosDigitais.Blocks.BL01_Login.mvc.js",
                headers={"Accept": "*/*"},
                check_status=False,
                capture={
                    "url_cloudflare": Regex(
                        CLOUDFLARE, default=LOGIN_SCREEN + "DataActionCheckUsarCloudFlare"
                    ),
                    "api_cloudflare": Regex(
                        CLOUDFLARE, group=2, default="OTGxLOOIYRPT4Yzxi6zCuA"
                    ),
                    "url_login": Regex(
                        DADOS_LOGIN, default=LOGIN_SCREEN + "DataActionGetDadosLogin"
                    ),
                    "api_login": Regex(
                        DADOS_LOGIN, group=2, default="kEIaQNU5n93i9Q026f_dlQ"
                    ),
                },
            ),
            # 2. login
            SetHeaders(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) "
                        "Gecko/20100101 Firefox/149.0"
                    ),
                    "Content-Type": "application/json; charset=UTF-8",
                    "Accept": "application/json",
                    "OutSystems-Client-Type": "Web",
                    "Origin": origem_auth,
                    "Referer": auth,
                    "X-CSRFToken": "",
                }
            ),
            Request("portal", "GET", auth),
            Request(
                "cloudflare",
                "POST",
                auth + "{url_cloudflare}",
                json=_login("{api_cloudflare}", "", ""),
                capture={
                    "csrf": Cookie("nr2Users", pattern=r"crf=([^;]+)", unquote_url=True)
                },
            ),
            SetHeaders({"X-CSRFToken": "{csrf}"}),
            Request(
                "login",
                "POST",
                auth + "{url_login}",
                json=_login("{api_login}", "{email}", "{senha}"),
                expect=(JsonEquals("data.FLG_Sucesso", True),),
                capture={"entrada": JsonField("data.URL_Gratuito")},
            ),
            DropHeaders(
                (
                    "Content-Type",
                    "Accept",
                    "OutSystems-Client-Type",
                    "Origin",
                    "Referer",
                    "X-CSRFToken",
                )
            ),
            # 3. busca e seleção da série
            Request("entrada", "GET", "{entrada}", capture=_campos_ocultos(False)),
            Request(
                "busca",
                "POST",
                pagina,
                data={
                    **busca,
                    "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeries",
                    "ctl00$butBuscarSeries": "OK",
                },
                headers=_ajax(pagina, origem_legacy),
                expect=(Contains(SERIE),),
                capture=_campos_ocultos(True),
            ),
            Request(
                "selecao",
                "POST",
                pagina,
                data={
                    **busca,
                    "ctl00$smg": "ctl00$updpBuscarSeries|ctl00$butBuscarSeriesOK",
                    "ctl00$dlsSerie$ctl00$chkSerieEscolhida": "on",
                    "ctl00$dlsSerie$ctl01$chkSerieEscolhida": "on",
                    "ctl00$butBuscarSeriesOK": "OK",
                },
                headers=_ajax(pagina, origem_legacy),
            ),
            # 4. consulta da série histórica
            Request(
                "consulta",
                "GET",
                consulta,
                expect=(Contains(SERIE),),
                capture=_campos_ocultos(False),
            ),
            Request(
                "serie_historica",
                "POST",
                consulta,
                data={
                    **_form_consulta(),
                    "ctl00$smg": (
                        "ctl00$cphConsulta$updpOpcoes|"
                        "ctl00$cphConsulta$rbtSerieHistorica"
                    ),
                    "__EVENTTARGET": "ctl00$cphConsulta$rbtSerieHistorica",
                },
                headers=_ajax(consulta, origem_legacy),
                capture=_campos_ocultos(True),
            ),
            Request(
                "visualizar",
                "POST",
                consulta,
                data={
                    **_form_consulta(),
                    "ctl00$smg": (
                        "ctl00$updpAreaConsulta|"
                        "ctl00$cphConsulta$butVisualizarResultado"
                    ),
                    "__EVENTTARGET": "",
                    "__EVENTVALIDATION": "{eventvalidation}",
                    "ctl00$cphConsulta$butVisualizarResultado": "Visualizar e salvar",
                },
                drop_empty=("__EVENTVALIDATION",),
                headers=_ajax(consulta, origem_legacy),
            ),
            # 5. download do CSV
            Request(
                "visualizacao",
                "GET",
                legacy + "visualizaconsulta.aspx",
                headers={"Referer": legacy},
            ),
            Request(
                "quadro",
                "GET",
                quadro,
                headers={"Referer": legacy},
                expect=(Contains(SERIE),),
                capture={
                    **_campos_ocultos(False),
                    **{
                        campo.lower(): Regex(
                            rf'name="{campo}"[^>]*value="(.*?)"',
                            unescape_html=True,
                            default="",
                        )
                        for campo in ("xgdvConsulta", "DXScript", "DXCss")
                    },
                },
            ),
            Download(
                "csv",
                "POST",
                quadro,
                data={
                    "__EVENTTARGET": "lbtSalvarCSV",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": "{viewstate}",
                    "__VIEWSTATEGENERATOR": "{viewstategenerator}",
                    "__EVENTVALIDATION": "{eventvalidation}",
                    "xgdvConsulta": "{xgdvconsulta}",
                    "DXScript": "{dxscript}",
                    "DXCss": "{dxcss}",
                },
                drop_empty=("__EVENTVALIDATION", "xgdvConsulta", "DXScript", "DXCss"),
                headers={"Referer": quadro},
                filename=f"{SERIE.lower()}.csv",
                content_types=("text/csv", "application/octet-stream"),
            ),
        ),
    )


DATASET = DatasetSpec(
    domain="fgv",
    dataset="icst",
    extractor=ExtractorConfig(source="http_session", session=fluxo()),
    converter=ConverterConfig(encoding="latin-1", delimiter=";"),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="icst_ingest_dag",
    # Diário às 06:00: a FGV publica uma vez por mês, sem data fixa.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fgv", "icst", "construcao", "confianca", "conjuntura", "ingestion"],
)
def icst_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> list[str]:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefixes: list[str]) -> str:
        return steps.convert_to_staging(DATASET, raw_prefixes)

    convert_to_staging(extract_to_raw())


dag_instance = icst_ingest_dag()
