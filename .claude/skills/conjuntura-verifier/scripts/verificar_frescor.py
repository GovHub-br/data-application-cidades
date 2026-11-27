"""Frescor do pipeline do conjuntura, ponta a ponta e só leitura: fonte → lake → ouro → Airflow.

Uso (na raiz do repositório, com VPN):
    .venv/bin/python .claude/skills/conjuntura-verifier/scripts/verificar_frescor.py [--sem-fonte]

Imprime uma tabela markdown por indicador. Nada aqui escreve: o lake e o banco são lidos,
o SFTP só é listado, o e-mail é aberto em modo leitura e o Airflow só recebe GET.
Credenciais: .env (DL_*, DB_DW_*_MCID) e infra/airflow/bootstrap/{connections,variables}.json.
"""

from __future__ import annotations

import argparse
import datetime as dt
import email
import imaplib
import io
import json
import re
import stat
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
AIRFLOW = "http://10.0.0.144:8080"


def _env() -> dict:
    from dotenv import dotenv_values

    return dotenv_values(ROOT / ".env")


def _json(name: str) -> dict:
    return json.loads((ROOT / "infra/airflow/bootstrap" / name).read_text())


def _get(url: str, headers: dict | None = None, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    return urllib.request.urlopen(req, timeout=timeout).read()


def _safe(fn):
    """Uma fonte que falha vira texto na tabela; a verificação continua."""

    def run(*a, **kw):
        try:
            return fn(*a, **kw)
        except Exception as exc:  # noqa: BLE001
            return f"erro: {type(exc).__name__}: {str(exc)[:80]}"

    return run


# --- fonte -----------------------------------------------------------------------------


@_safe
def sgs(code: str) -> str:
    rows = json.loads(
        _get(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/1?formato=json")
    )
    return rows[-1]["data"][3:]  # dd/mm/aaaa -> mm/aaaa


@_safe
def olinda_credito_pib() -> str:
    url = ("https://olinda.bcb.gov.br/olinda/servico/MercadoImobiliario/versao/v1/odata/mercadoimobiliario"
           "?%24filter=Info%20eq%20%27indices_imobiliario_pib_br%27&%24orderby=Data%20desc&%24top=1&%24format=json")  # fmt: skip
    return json.loads(_get(url, {"Accept": "application/json"}))["value"][0]["Data"][:7]


@_safe
def sidra(agregado: int) -> str:
    periods = json.loads(
        _get(f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos")
    )
    return periods[-1]["id"]


def _xlsx_last_month(content: bytes, sheet: str | None = None) -> str:
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    last = None
    for ws in [wb[sheet]] if sheet else wb.worksheets:
        for row in ws.iter_rows(values_only=True):
            dates = [v for v in row[:3] if isinstance(v, (dt.datetime, dt.date))]
            numbers = [v for v in row[1:8] if isinstance(v, (int, float)) and v]
            if dates and numbers:
                last = max(last or dates[0], dates[0])
    return last.strftime("%Y-%m") if last else "sem data"


@_safe
def abecip(page: str, sheet: str) -> str:
    html = _get(f"https://www.abecip.org.br/credito-imobiliario/indicadores/{page}").decode(
        "utf-8", "ignore"
    )
    link = re.search(
        r'href="(https://www\.abecip\.org\.br/download\?file=[^"]+\.xlsx)"', html
    ).group(1)
    return _xlsx_last_month(_get(link), sheet)


@_safe
def abecip_pdf() -> str:
    html = _get("https://www.abecip.org.br/imprensa/informativos-mensais").decode("utf-8", "ignore")
    return max(re.findall(r"data-abecip-(\d{4}-\d{2})", html))


@_safe
def fipezap() -> str:
    return _xlsx_last_month(_get("https://downloads.fipe.org.br/indices/fipezap/fipezap-serieshistoricas.xlsx"),
                            "Índice FipeZAP")  # fmt: skip


@_safe
def sinduscon_incc() -> str:
    url = "https://sindusconpr.com.br/economia/indices-economicos/incc-m-fgv/9547-serie-historica-incc-m-fgv/"
    return _xlsx_last_month(_get(url))


@_safe
def pdet() -> str:
    import ftplib

    with ftplib.FTP("ftp.mtps.gov.br", timeout=60, encoding="latin-1") as ftp:
        ftp.login()
        base = "/pdet/microdados/NOVO CAGED"
        year = max(n.rsplit("/", 1)[-1] for n in ftp.nlst(base) if n.rsplit("/", 1)[-1].isdigit())
        return max(n.rsplit("/", 1)[-1] for n in ftp.nlst(f"{base}/{year}"))


@_safe
def sftp_latest(conn_id: str, folder: str, pattern: str) -> str:
    import paramiko

    c = _json("connections.json")[conn_id]
    t = paramiko.Transport((c["host"], int(c.get("port") or 22)))
    t.connect(username=c["login"], password=c["password"])
    try:
        s = paramiko.SFTPClient.from_transport(t)
        names = [a.filename for a in s.listdir_attr(folder) if not stat.S_ISDIR(a.st_mode or 0)]
    finally:
        t.close()
    hits = sorted(n for n in names if re.fullmatch(pattern, n))
    return hits[-1] if hits else "nenhum arquivo"


@_safe
def tesouro_emails(days: int = 20) -> str:
    c = _json("connections.json")["tesouro_imap"]
    extra = c["extra"] if isinstance(c["extra"], dict) else json.loads(c["extra"])
    m = imaplib.IMAP4_SSL(c["host"])
    m.login(c["login"], c["password"])
    m.select("INBOX", readonly=True)
    since = (dt.date.today() - dt.timedelta(days=days)).strftime("%d-%b-%Y")
    _, ids = m.search(None, "FROM", f'"{extra["sender_email"]}"', "SINCE", since)
    last: dict[str, str] = {}
    if ids[0]:
        _, data = m.fetch(b",".join(ids[0].split()), "(BODY.PEEK[HEADER.FIELDS (DATE SUBJECT)])")
        for part in data:
            if isinstance(part, tuple):
                h = email.message_from_bytes(part[1])
                day = email.utils.parsedate_to_datetime(h["Date"]).strftime("%d/%m")
                last[str(h["Subject"])] = day
    m.logout()
    return (
        "; ".join(f"{s}: {d}" for s, d in sorted(last.items())) or f"nenhum e-mail em {days} dias"
    )


# --- lake e banco ----------------------------------------------------------------------


def _duckdb(env: dict):
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"CREATE SECRET (TYPE s3, KEY_ID '{env['DL_ACCESS_KEY']}', SECRET '{env['DL_SECRET_KEY']}', "
                f"ENDPOINT '{env['DL_ENDPOINT']}', URL_STYLE 'path', USE_SSL false)")  # fmt: skip
    return con


def lake(con, bucket: str, path: str, expr: str, where: str = "true") -> str:
    try:
        rel = f"read_parquet('s3://{bucket}/{path}', union_by_name=true, hive_partitioning=false, filename=true)"
        agg = expr if expr.startswith("max(") else f"max({expr})"
        value = con.execute(f"select {agg}::varchar from {rel} where {where}").fetchone()[0]
        return (value or "vazio")[:10]
    except Exception as exc:  # noqa: BLE001
        return f"erro: {str(exc).splitlines()[0][:60]}"


def ouro(cur, table: str, col: str = "data_referencia", where: str = "true") -> str:
    try:
        cur.execute(f"select max({col})::text from ouro.{table} where {where}")
        return (cur.fetchone()[0] or "vazio")[:10]
    except Exception as exc:  # noqa: BLE001
        cur.connection.rollback()
        return f"erro: {str(exc).splitlines()[0][:60]}"


def airflow_states() -> dict:
    try:
        body = json.dumps({"username": "airflow", "password": "airflow"}).encode()
        req = urllib.request.Request(
            f"{AIRFLOW}/auth/token", body, {"Content-Type": "application/json"}
        )
        tok = json.load(urllib.request.urlopen(req, timeout=15))["access_token"]
        auth = {"Authorization": f"Bearer {tok}"}
        dags = json.loads(_get(f"{AIRFLOW}/api/v2/dags?limit=200&exclude_stale=true", auth))["dags"]
        out = {}
        for d in dags:
            runs = json.loads(
                _get(
                    f"{AIRFLOW}/api/v2/dags/{d['dag_id']}/dagRuns?limit=1&order_by=-run_after", auth
                )
            )
            last = runs["dag_runs"][0] if runs["dag_runs"] else {}
            out[d["dag_id"]] = ("pausada " if d["is_paused"] else "") + (
                f"{last.get('state')} {str(last.get('run_after'))[:10]}" if last else "nunca rodou"
            )
        return out
    except Exception as exc:  # noqa: BLE001
        return {"_erro": f"Airflow inacessível: {exc}"}


# --- os indicadores --------------------------------------------------------------------

# data no nome do arquivo entregue (Base_PF_FGTS_20261007, MC20261119, PMCMV_..._2026_11_20)
DATA_ARQ = "max(replace(regexp_extract(filename, '(\\d{8}|\\d{4}_\\d{2}_\\d{2})(\\.zip|\\.parquet|\\.csv|__)', 1), '_', ''))"


# (nome, fonte, (caminho no lake, expressão, filtro) ou None, (tabela ouro, coluna) ou None, DAG)
def indicadores(env: dict, sem_fonte: bool):
    series = _json("variables.json")["BACEN_SERIES"]["value"]
    f = (lambda fn, *a: "—") if sem_fonte else (lambda fn, *a: fn(*a))
    st = "snh/staging"
    return [
        (
            "IBGE – PIB construção",
            f(sidra, 5932),
            (
                f"{st}/ibge/pib_construcao/load_id=*/*.parquet",
                "periodo",
                "valor not in ('...','-','')",
            ),
            ("ouro_conjuntura_pib_construcao_civil_pct",),
            "ibge_ingest_dag",
        ),
        (
            "IBGE – SINAPI",
            f(sidra, 2296),
            (f"{st}/ibge/sinapi/load_id=*/*.parquet", "periodo", "valor not in ('...','-','')"),
            ("ouro_conjuntura_sinapi",),
            "ibge_ingest_dag",
        ),
        (
            "IBGE – PNAD construção",
            f(sidra, 6323),
            (f"{st}/ibge/pnad_trabalho_construcao/load_id=*/*.parquet", "periodo", "true"),
            ("ouro_conjuntura_pnad_ocupados",),
            "ibge_ingest_dag",
        ),
        (
            "BACEN – crédito PF/PJ (SGS)",
            f(sgs, series["pf_concessoes_rs_mi"]),
            (
                f"{st}/bacen/financiamentos_imobiliarios/load_id=*/*.parquet",
                "try_strptime(data,'%d/%m/%Y')",
                "tipo='pf_concessoes_rs_mi'",
            ),
            ("ouro_conjuntura_financiamentos_imobiliarios_pf_pj", "data"),
            "bacen_sgs_ingest_dag",
        ),
        (
            "BACEN – crédito/PIB (Olinda)",
            f(olinda_credito_pib),
            (
                f"{st}/bacen/credito_imobiliario_pib/load_id=*/*.parquet",
                "try_cast(data as date)",
                "info='indices_imobiliario_pib_br'",
            ),
            ("ouro_conjuntura_credito_pib", "data"),
            "bacen_credito_pib_ingest_dag",
        ),
        (
            "INCC-M (SGS 7456)",
            f(sgs, "7456"),
            (
                f"{st}/bacen/financiamentos_imobiliarios/load_id=*/*.parquet",
                "try_strptime(data,'%d/%m/%Y')",
                "tipo='incc_m_var_mes_pct'",
            ),
            ("ouro_conjuntura_incc_m", "mes"),
            "bacen_sgs_ingest_dag",
        ),
        (
            "INCC-M (Sinduscon-PR, parado)",
            f(sinduscon_incc),
            (f"{st}/fgv/incc_m/load_id=*/*.parquet", "try_cast(mes as date)", "indice is not null"),
            None,
            "incc_m_ingest_dag",
        ),
        (
            "FGV – ICST (login)",
            "ver lake",
            (
                f"{st}/fgv/icst/load_id=*/*.parquet",
                "try_strptime(mes::varchar,['%m/%Y','%Y-%m-%d','%Y-%m'])",
                "true",
            ),
            ("ouro_conjuntura_icst",),
            "icst_ingest_dag",
        ),
        (
            "ABECIP – SBPE unidades",
            f(abecip, "financiamento", "BD_Unidades"),
            (
                f"{st}/abecip/financiamentos_modalidade/load_id=*/*.parquet",
                "try_cast(data_referencia as date)",
                "try_cast(unidades_total as double)>0",
            ),
            ("ouro_conjuntura_uh_condicao_uso",),
            "abecip_financiamentos_ingest_dag",
        ),
        (
            "ABECIP – poupança SBPE",
            f(abecip, "caderneta-de-poupanca", "SBPE_Mensal"),
            (
                f"{st}/abecip/poupanca_sbpe_mensal/load_id=*/*.parquet",
                "try_cast(data_referencia as date)",
                "try_cast(saldo as double)>0",
            ),
            ("ouro_conjuntura_saldo_poupanca",),
            "abecip_poupanca_trimestral_ingest_dag",
        ),
        (
            "ABECIP – por banco (PDF/OCR)",
            f(abecip_pdf),
            (
                f"{st}/abecip/financiamentos_por_instituicao.parquet",
                "competencia_referencia",
                "true",
            ),
            ("ouro_conjuntura_financiamentos_instituicao",),
            "abecip_instituicoes_ingest_dag",
        ),
        (
            "FipeZap – locação",
            f(fipezap),
            (
                f"{st}/fipe/indice_locacao/load_id=*/*.parquet",
                "try_cast(data_referencia as date)",
                "try_cast(imoveis_residenciais_locacao_numero_indice_total as double)>0",
            ),
            ("ouro_conjuntura_fipezap",),
            "fipezap_trimestral_ingest_dag",
        ),
        (
            "B3 – IMOB (Alpha Vantage)",
            "diária",
            (
                f"{st}/infomoney/acoes_imob/load_id=*/*.parquet",
                "try_cast(data_pregao as date)",
                "true",
            ),
            ("ouro_conjuntura_indice_imob",),
            "infomoney_imob",
        ),
        (
            "MTE – Novo CAGED (PDET)",
            f(pdet),
            (f"{st}/novo_caged/pdet/*/*/load_id=*/*.parquet", "competenciamov", "true"),
            ("ouro_conjuntura_empregos_caged",),
            "novo_caged_pdet_ingest_dag",
        ),
        (
            "GEAVO – FGTS-PJ",
            f(sftp_latest, "sftp_geavo", "/GEAVO", r"Base_PJ_FGTS_\d{8}\.zip"),
            (f"{st}/sftp/caixa.geavo/GEAVO/**/Base_PJ_FGTS_*.parquet", DATA_ARQ, "true"),
            ("ouro_conjuntura_financiamentos_habitacionais",),
            "sftp_ingest_dag",
        ),
        (
            "GEAVO – FGTS-PF",
            f(sftp_latest, "sftp_geavo", "/GEAVO", r"Base_PF_FGTS_\d{8}\.zip"),
            (f"{st}/sftp/caixa.geavo/GEAVO/**/Base_PF_FGTS_*.parquet", DATA_ARQ, "true"),
            None,
            "sftp_ingest_dag",
        ),
        (
            "GEAVO – CCI/CCA (MC)",
            f(sftp_latest, "sftp_geavo", "/GEAVO", r"MC\d{8}\.zip"),
            (
                f"{st}/sftp/caixa.geavo/GEAVO/**/*[Cc][Cc][Ii]_[Aa]nalitico.parquet",
                DATA_ARQ,
                "true",
            ),
            ("ouro_conjuntura_financiamento_pf_faixa",),
            "sftp_ingest_dag",
        ),
        (
            "GEFUS – Fundo Social",
            f(sftp_latest, "sftp_mcid", "/home/fabrica/GEFUS", r"PMCMV_FAIXA3_MCID_.*\.csv"),
            (f"{st}/sftp/fabrica/GEFUS/**/PMCMV_FAIXA3_MCID_*.parquet", DATA_ARQ, "true"),
            ("ouro_conjuntura_fundo_social",),
            "sftp_ingest_dag",
        ),
        (
            "SIAFI – OGU (e-mail)",
            f(tesouro_emails),
            (f"{st}/siafi-tesouro-gerencial/**/*.parquet", "_ingested_at", "true"),
            ("ouro_conjuntura_ogu", "dt_referencia_extracao"),
            "orcamento_mcid_por_acao_com_data_ingest_dag",
        ),
        (
            "Manual – CBIC",
            "PDF da coletiva",
            None,
            (
                "ouro_conjuntura_boletim_p1_cbic_lancamentos_e_vendas_totais",
                "right(edicao, 4) || 'T' || left(edicao, 1)",  # ano antes do trimestre: ordena
                """"Lançamentos TOTAL" is not null and periodo = 'Trimestre selecionado'""",
            ),
            "—",
        ),
        (
            "Manual – construtoras",
            "releases de RI",
            None,
            ("ouro_conjuntura_balancos_empresas",),
            "—",
        ),
        ("Manual – Canal FGTS", "planilha", None, ("ouro_conjuntura_canal_fgts",), "—"),
        ("Manual – funding", "planilha", None, ("ouro_conjuntura_funding",), "—"),
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--sem-fonte", action="store_true", help="não consulta as fontes externas (mais rápido)"
    )
    args = ap.parse_args()
    env = _env()
    import psycopg2

    pg = psycopg2.connect(host=env["DB_DW_HOST_MCID"], port=env["DB_DW_PORT_MCID"], user=env["DB_DW_USER_MCID"],
                          password=env["DB_DW_PASSWORD_MCID"], dbname=env["DB_DW_DBNAME_MCID"], connect_timeout=10)  # fmt: skip
    pg.set_session(readonly=True)
    cur = pg.cursor()
    con = _duckdb(env)
    states = airflow_states()
    print(f"# Frescor do conjuntura — {dt.datetime.now():%d/%m/%Y %H:%M}\n")
    print(
        "| Indicador | Fonte (último publicado) | Lake (staging) | Banco (ouro) | DAG (última execução) |"
    )
    print("|---|---|---|---|---|")
    for nome, fonte, lk, ou, dag in indicadores(env, args.sem_fonte):
        lake_v = lake(con, env["DL_BUCKET"], *lk) if lk else "—"
        ouro_v = ouro(cur, *ou) if ou else "—"
        print(f"| {nome} | {fonte} | {lake_v} | {ouro_v} | {dag}: {states.get(dag, '—')} |")
    cur.execute("select max(medido_em)::text from ouro.ouro_conjuntura_qualidade_inventario")
    print(f"\nÚltimo `dbt run` do conjuntura (inventário de qualidade): {cur.fetchone()[0]}")
    print(f"conjuntura_dag: {states.get('conjuntura_dag', '—')}")
    if "_erro" in states:
        print(states["_erro"], file=sys.stderr)


if __name__ == "__main__":
    main()
