"""The single staging writer and its readers (ADR-0004, ADR-0002 R2).

`READERS` maps a reader name (the `reader` of the contract) to a function that turns one
raw object into staging Parquet records. `auto` is the generic conversion that already
exists in scripts/conversao/ (CSV, TXT, XLSX and the CSV of each Access table): it writes
every column as text, with the lineage columns and `_load_id`. Sources that do not deliver
a ready table (the FGV XLSX, the IBGE JSON, the Power BI answer of the Novo CAGED) register
their reader here; a reader only gives the data a table shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    import pandas as pd

READERS: dict[str, Callable[..., list]] = {}


def reader(name: str) -> Callable:
    def register(func: Callable) -> Callable:
        READERS[name] = func
        return func

    return register


@reader("auto")
def auto(minio, key: str, execution_id: str, apply: bool, converted: set, **options) -> list:
    """Generic conversion by file extension (existing code, unchanged)."""
    # shortcut: nome curto, como no tasks.transform (scripts/conversao roda também por CLI)
    from conversao.processamento import processar_objeto

    return processar_objeto(
        minio,
        key,
        execution_id,
        apply,
        converted,
        options.get("chunksize", 200_000),
        options.get("bad_mode", "skip"),
    )


# --- API sources: the original file -> a table, nothing else -----------------------------
# The `transform` task of each API DAG writes the result with plugins.datalake.api. A
# parser only gives the delivered file its table shape: no typing, no `dt_ingest` (the
# moment of the load is `_load_id`/`_ingested_at`).

PARSERS: dict[str, Callable[[str], "pd.DataFrame"]] = {}


def parser(name: str) -> Callable:
    def register(func: Callable) -> Callable:
        PARSERS[name] = func
        return func

    return register


def parse(name: Optional[str], path: str) -> "pd.DataFrame":
    if name not in PARSERS:
        raise KeyError(f"no parser {name!r}; register it in plugins.datalake.readers")
    return PARSERS[name](path)


@parser("fgv_icst_csv")
def fgv_icst_csv(path: str) -> "pd.DataFrame":
    """ICST exported by FGVDados: `;`, latin-1, one header row."""
    import pandas as pd

    return pd.read_csv(
        path, sep=";", encoding="latin1", header=0, dtype=str,
        names=["mes", "icst_com_ajuste_sazonal", "icst_sem_ajuste_sazonal"],
    )  # fmt: skip


@parser("sinduscon_incc_xlsx")
def sinduscon_incc_xlsx(path: str) -> "pd.DataFrame":
    """INCC-M of the Sinduscon-PR: 3 title rows, columns A:E, a "Fonte FGV" footer."""
    import pandas as pd

    df = pd.read_excel(
        path, skiprows=3, usecols="A:E",
        names=["mes", "indice", "var_mes", "var_ano", "var_12_meses"],
    )  # fmt: skip
    df = df.dropna(subset=["mes"])
    return df[~df["mes"].astype(str).str.contains("Fonte", case=False, na=False)]


class SourceLayoutChanged(Exception):
    """The delivered file no longer has the layout the parser reads by position.

    Raised by the layout checks below (an identity of the data stops holding): the file
    stays in raw and the transform task fails with the cause, instead of writing a
    series with swapped columns.
    """


def _xlsx_sheet(path: str, name: str) -> str:
    """The sheet called `name`, or the only one starting with it (the source renames)."""
    import pandas as pd

    sheets = pd.ExcelFile(path).sheet_names
    if name in sheets:
        return name
    candidates = [s for s in sheets if s.strip().lower().startswith(name.lower())]
    if len(candidates) != 1:
        raise SourceLayoutChanged(f"sheet {name!r} not found; prefix matched {candidates}")
    return candidates[0]


ABECIP_FINANCING = ["data_referencia", "unidades_construcao", "unidades_aquisicao",
                    "unidades_total", "valor_construcao_milhoes", "valor_aquisicao_milhoes",
                    "valor_total_milhoes"]  # fmt: skip


@parser("abecip_financiamentos_xlsx")
def abecip_financiamentos_xlsx(path: str) -> "pd.DataFrame":
    """SBPE financing by purpose (sheet BD_Unidades), monthly since 2002.

    The header spans merged rows, so the columns are read by position and checked by
    Total == Construção + Aquisição in units and in R$ (a moved column breaks it).
    """
    import pandas as pd

    raw = pd.read_excel(path, sheet_name=_xlsx_sheet(path, "BD_Unidades"), header=None)
    df = raw.iloc[:, : len(ABECIP_FINANCING)].copy()
    df.columns = ABECIP_FINANCING
    df["data_referencia"] = pd.to_datetime(df["data_referencia"], errors="coerce")
    df = df[df["data_referencia"].notna()].copy()  # only rows with a date: no header
    for col in ABECIP_FINANCING[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[df["unidades_total"].notna()].copy()  # future months come as empty rows
    for total, parts in (
        ("unidades_total", ["unidades_construcao", "unidades_aquisicao"]),
        ("valor_total_milhoes", ["valor_construcao_milhoes", "valor_aquisicao_milhoes"]),
    ):
        off = ((df[total] - df[parts].sum(axis=1)).abs() > df[total].abs() * 0.001 + 1).sum()
        if off:
            raise SourceLayoutChanged(f"{off} rows where {total} != {' + '.join(parts)}")
    df["data_referencia"] = df["data_referencia"].dt.strftime("%Y-%m-%d")
    return df.reset_index(drop=True)


ABECIP_SAVINGS = ["data_referencia", "deposito", "retirada", "captacao_liquida_valor",
                  "captacao_liquida_pct", "rendimento", "saldo"]  # fmt: skip


@parser("abecip_poupanca_xlsx")
def abecip_poupanca_xlsx(path: str) -> "pd.DataFrame":
    """SBPE savings (sheet SBPE_Mensal): monthly rows only, read by position from row 6.

    Checks captação líquida == depósito - retirada (exact) and the saldo evolution
    (holds in all but a few methodology breaks, so by proportion).
    """
    from datetime import datetime

    import pandas as pd

    raw = pd.read_excel(path, sheet_name=_xlsx_sheet(path, "SBPE_Mensal"), header=None)
    df = raw.iloc[6:, range(len(ABECIP_SAVINGS))].copy()
    df.columns = ABECIP_SAVINGS
    df = df[df["data_referencia"].apply(lambda x: isinstance(x, datetime))].copy()  # no totals
    df = df[~((df["captacao_liquida_valor"] == 0) & df["saldo"].isna())].copy()  # future
    df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.strftime("%Y-%m-%d")
    for col in ABECIP_SAVINGS[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    d = df.sort_values("data_referencia").reset_index(drop=True)
    net = (d["deposito"] - d["retirada"] - d["captacao_liquida_valor"]).abs().dropna()
    if len(net) and (net > 0.01).mean() > 0.01:
        raise SourceLayoutChanged(f"captação != depósito - retirada in {(net > 0.01).mean():.0%}")
    balance = d["saldo"].shift(1) + d["captacao_liquida_valor"] + d["rendimento"] - d["saldo"]
    balance = balance.abs().dropna()
    if len(balance) and (balance > 1.0).mean() > 0.10:
        raise SourceLayoutChanged(f"saldo evolution off in {(balance > 1.0).mean():.0%}")
    return df.reset_index(drop=True)


FIPEZAP_COLUMNS = {1: "data_referencia",
                   22: "imoveis_residenciais_locacao_numero_indice_total",
                   27: "imoveis_residenciais_locacao_var_mensal_total",
                   32: "imoveis_residenciais_locacao_var_ano_total"}  # fmt: skip


@parser("fipezap_xlsx")
def fipezap_xlsx(path: str) -> "pd.DataFrame":
    """FipeZAP rental index (sheet "Índice FipeZAP"), read by position from row 4.

    Checks that the monthly and 12-month variations agree with the index itself: a
    moved column breaks it in most months (a few revisions do not).
    """
    import pandas as pd

    raw = pd.read_excel(path, sheet_name="Índice FipeZAP", header=None)
    df = raw.iloc[4:, list(FIPEZAP_COLUMNS)].copy()
    df.columns = list(FIPEZAP_COLUMNS.values())
    df = df[pd.notna(df["data_referencia"])].copy()  # footer rows have no date
    df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.strftime("%Y-%m-%d")
    index, *variations = list(FIPEZAP_COLUMNS.values())[1:]
    for col in (index, *variations):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    d = df.sort_values("data_referencia").reset_index(drop=True)
    for col, lag in zip(variations, (1, 12)):
        expected = d[index] / d[index].shift(lag) - 1
        comparable = expected.notna() & d[col].notna()
        if comparable.sum() < lag + 12:
            continue  # too short to conclude anything
        off = ((expected - d[col]).abs() > 0.0015) & comparable
        if off.sum() / comparable.sum() > 0.20:
            raise SourceLayoutChanged(f"{col} disagrees with the index in most months")
    return df.reset_index(drop=True)


@parser("bacen_olinda_json")
def bacen_olinda_json(path: str) -> "pd.DataFrame":
    """One indicator of the BCB Olinda OData (MercadoImobiliario): `value` -> rows."""
    import json

    import pandas as pd

    with open(path, encoding="utf-8") as f:
        rows = json.load(f)["value"]
    return pd.DataFrame(
        [{"info": r["Info"], "data": r["Data"], "valor": r["Valor"]} for r in rows],
        columns=["info", "data", "valor"],
    )


IBGE_COLUMNS = ["variavel_id", "variavel_nome", "localidade_id", "localidade_nome",
                "classificacao_id", "classificacao_nome", "categoria_id", "categoria_nome",
                "unidade", "periodo", "valor"]  # fmt: skip


@parser("ibge_agregados_json")
def ibge_agregados_json(path: str) -> "pd.DataFrame":
    """IBGE aggregates API v3: variable -> result -> series -> period, one row each.

    Several classifications of one result are joined with `|`. `valor` stays as
    delivered (`...`, `-` and `X` included): the prata decides what is a number.
    """
    import json

    import pandas as pd

    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    rows = []
    for variable in payload:
        for result in variable["resultados"]:
            classes = result.get("classificacoes", [])
            categories = [kv for c in classes for kv in (c.get("categoria") or {}).items()]
            context = {
                "classificacao_id": "|".join(c["id"] for c in classes) or "0",
                "classificacao_nome": " | ".join(c["nome"] for c in classes),
                "categoria_id": "|".join(k for k, _ in categories) or "0",
                "categoria_nome": " | ".join(v for _, v in categories),
            }
            for series in result["series"]:
                for period, value in series["serie"].items():
                    rows.append({
                        "variavel_id": str(variable["id"]),
                        "variavel_nome": variable["variavel"],
                        "localidade_id": str(series["localidade"]["id"]),
                        "localidade_nome": series["localidade"]["nome"],
                        **context,
                        "unidade": variable["unidade"],
                        "periodo": str(period),
                        "valor": value,
                    })  # fmt: skip
    return pd.DataFrame(rows, columns=IBGE_COLUMNS)


@parser("bacen_sgs_json")
def bacen_sgs_json(path: str) -> "pd.DataFrame":
    """One SGS series as the API delivers it (`[{data, valor}]`); the series name is the
    file name, because the answer does not carry it."""
    import json
    from pathlib import Path

    import pandas as pd

    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):  # the SGS sometimes answers 200 with an HTML page
        raise SourceLayoutChanged(f"SGS answer is not a list: {type(rows).__name__}")
    df = pd.DataFrame(rows, columns=["data", "valor"])
    df.insert(0, "tipo", Path(path).stem)
    return df


NOVO_CAGED_MEASURES = ["admitidos", "desligados", "saldo", "estoque", "variacao"]


def novo_caged_values(answer: dict) -> list:
    """The five measures of one month in a Power BI answer (the order of the query)."""
    error = ((answer.get("results") or [{}])[0].get("result") or {}).get("error") or {}
    if error:  # the report answers 200 with the failure inside (e.g. the model is offline)
        raise SourceLayoutChanged(
            f"Power BI: {error.get('errorCode')}: {error.get('message', '')[:300]}"
        )
    try:
        values = answer["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][0]["C"]
    except (KeyError, IndexError, TypeError) as exc:
        raise SourceLayoutChanged(f"Power BI answer without the month data: {exc!r}") from exc
    if len(values) != len(NOVO_CAGED_MEASURES):
        raise SourceLayoutChanged(f"expected {len(NOVO_CAGED_MEASURES)} measures, got {values}")
    return values


@parser("novo_caged_powerbi_json")
def novo_caged_powerbi_json(path: str) -> "pd.DataFrame":
    """One month of the Novo CAGED report: year and month come from the file name
    (`<year>-<month>.json`), the measures from the answer."""
    import json
    from pathlib import Path

    import pandas as pd

    year, month = Path(path).stem.split("-")
    with open(path, encoding="utf-8") as f:
        values = novo_caged_values(json.load(f))
    return pd.DataFrame([[year, month, *values]], columns=["ano", "mes", *NOVO_CAGED_MEASURES])


MRV_SHEETS = ["Dados Oper. MRV&Co | Oper.Data", "Dados Oper. MRV&Co | Oper.D"]


def _mrv_block(path: str, section: str, offsets: list[int], names: list[str]) -> "pd.DataFrame":
    """One block of the MRV operational sheet, transposed to one row per quarter.

    The sheet has a section title in column 0 ("Lançamentos %MRV"), then the subgroup
    "MRV Incorporação"; the KPIs are rows at fixed offsets below it, the quarters are the
    columns (header in row 1). The sheet name changes between releases: a known name
    first, the "dados oper" prefix only when it is the only match.
    """
    import pandas as pd

    sheets = pd.ExcelFile(path).sheet_names
    sheet = next((s for s in MRV_SHEETS if s in sheets), None)
    if sheet is None:
        candidates = [s for s in sheets if s.lower().startswith("dados oper")]
        if len(candidates) != 1:
            raise SourceLayoutChanged(f"operational sheet not found; prefix matched {candidates}")
        sheet = candidates[0]
    raw = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)
    first = raw[0].astype(str)
    found = raw[first.str.contains(section, case=False, na=False, regex=False)].index
    if not len(found):
        raise SourceLayoutChanged(f"section {section!r} not found")
    below = raw.loc[found[0] :]
    sub = below[below[0].astype(str).str.contains("MRV Incorporação", case=False, na=False)].index
    if not len(sub):
        raise SourceLayoutChanged(f"subgroup 'MRV Incorporação' not found below {section!r}")
    block = raw.iloc[[raw.index.get_loc(sub[0]) + o for o in offsets], 2:].copy()
    block.index = names
    block.columns = raw.iloc[1, 2:].astype(str).tolist()
    table = block.T.reset_index()
    table.columns = ["periodo", *names]
    # "1T26 / 1Q26" -> "1T26"; a float header like "1.0" loses the ".0"
    table["periodo"] = table["periodo"].map(lambda p: str(p).split(" / ")[0].strip())
    table["periodo"] = table["periodo"].str.replace(".0", "", regex=False)
    return table[~table[names[0]].isin(["nan", "None", ""]) & table[names[0]].notna()]


@parser("mrv_lancamentos_xlsx")
def mrv_lancamentos_xlsx(path: str) -> "pd.DataFrame":
    names = ["vgv_lancamentos_milhoes", "unidades", "preco_medio_unidade_mil"]
    return _mrv_block(path, "Lançamentos %MRV", [1, 2, 3], names)


@parser("mrv_vendas_xlsx")
def mrv_vendas_xlsx(path: str) -> "pd.DataFrame":
    names = ["vendas_liquidas_milhoes", "unidades", "preco_medio_unidade_mil"]
    return _mrv_block(path, "Vendas Líquidas %MRV", [1, 4, 5], names)


INFOMONEY_COLUMNS = ["symbol", "data_pregao", "open", "high", "low", "close", "volume"]


@parser("alphavantage_daily_json")
def alphavantage_daily_json(path: str) -> "pd.DataFrame":
    """TIME_SERIES_DAILY of the Alpha Vantage: one row per trading day, values as delivered.

    The answer brings only the last ~100 days (`compact`): the history lives in the
    earlier loads, which the prata joins.
    """
    import json

    import pandas as pd

    with open(path, encoding="utf-8") as f:
        answer = json.load(f)
    if "Time Series (Daily)" not in answer:  # rate limit or wrong key: a note, not data
        raise SourceLayoutChanged(f"answer without 'Time Series (Daily)': {list(answer)}")
    symbol = answer["Meta Data"]["2. Symbol"]
    rows = [
        [symbol, day, v["1. open"], v["2. high"], v["3. low"], v["4. close"], v["5. volume"]]
        for day, v in answer["Time Series (Daily)"].items()
    ]
    return pd.DataFrame(rows, columns=INFOMONEY_COLUMNS)


@parser("infomoney_postgres_csv")
def infomoney_postgres_csv(path: str) -> "pd.DataFrame":
    """The one-time export of the Infomoney history (prata_conjuntura_infomoney_imob, the
    last build with the whole series, from before the lake kept every load), with the same
    columns as the API reader."""
    import pandas as pd

    return pd.read_csv(path, dtype=str, keep_default_na=False)[INFOMONEY_COLUMNS]


TESOURO_COLUMNS = [
    "unidade_orcamentaria_codigo", "unidade_orcamentaria_nome", "acao_governo_codigo",
    "acao_governo_nome", "programa_governo_codigo", "programa_governo_nome",
    "plano_orcamentario_codigo", "plano_orcamentario_funcao", "plano_orcamentario_subfuncao",
    "plano_orcamentario_programa", "plano_orcamentario_acao", "plano_orcamentario_medida",
    "plano_orcamentario_descricao", "elemento_despesa_codigo", "elemento_despesa_nome",
    "orgao_uge_codigo", "orgao_uge_nome", "uge_matriz_filial", "ug_executora_codigo",
    "ug_executora_nome", "fixacao_despesa_loa", "dotacao_inicial", "dotacao_atualizada",
    "credito_disponivel", "despesas_empenhadas", "despesas_empenhadas_a_liquidar",
    "despesas_liquidadas_a_pagar", "despesas_pagas", "restos_a_pagar_inscritos",
    "restos_a_pagar_pagos",
]  # fmt: skip


@parser("tesouro_dotacao_zip")
def tesouro_dotacao_zip(path: str) -> "pd.DataFrame":
    """The Tesouro Gerencial report e-mailed as a ZIP: one UTF-16 TSV with 12 title rows.

    Columns by position (the report has no usable header); everything as text, so codes
    like `0E64` keep their letters and leading zeros.
    """
    import io
    import zipfile

    import pandas as pd

    with zipfile.ZipFile(path) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise SourceLayoutChanged(f"ZIP without a CSV: {archive.namelist()}")
        text = archive.read(members[0]).decode("utf-16")
    df = pd.read_csv(
        io.StringIO(text), sep="\t", skiprows=12, header=None, dtype=str,
        engine="python", on_bad_lines="skip",
    )  # fmt: skip
    extra = df.iloc[:, len(TESOURO_COLUMNS) :]
    if df.shape[1] < len(TESOURO_COLUMNS) or extra.notna().any().any():
        raise SourceLayoutChanged(f"expected {len(TESOURO_COLUMNS)} columns, got {df.shape[1]}")
    df = df.iloc[:, : len(TESOURO_COLUMNS)]  # a trailing tab gives an empty extra column
    df.columns = TESOURO_COLUMNS
    return df


# "Orçamento MCid por ação (com data)": 24 dimensions (as in the report header) and the 10
# metrics, keyed by the Item Informação code of the second header row
TESOURO_ACAO_DIMENSIONS = {
    "dia_lancamento": "Dia Lançamento",
    "acao_governo_codigo": "Ação Governo Código",
    "acao_governo_nome": "Ação Governo Nome",
    "programa_governo_codigo": "Programa Governo Código",
    "programa_governo_nome": "Programa Governo Nome",
    "fonte_recursos_detalhada_codigo": "Fonte Recursos Detalhada Código",
    "fonte_recursos_detalhada_nome": "Fonte Recursos Detalhada Nome",
    "pt": "PT",
    "ptres": "PTRES",
    "plano_orcamentario_uo": "Plano Orçamentário Código UO",
    "plano_orcamentario_funcao": "Plano Orçamentário Código Função",
    "plano_orcamentario_subfuncao": "Plano Orçamentário Código Subfunção",
    "plano_orcamentario_programa": "Plano Orçamentário Código Programa",
    "plano_orcamentario_acao": "Plano Orçamentário Código Ação",
    "plano_orcamentario_codigo": "Plano Orçamentário Código PO",
    "plano_orcamentario_nome": "Plano Orçamentário Nome",
    "ug_executora_codigo": "UG Executora Código",
    "ug_executora_nome": "UG Executora Nome",
    "ug_responsavel_codigo": "UG Responsável Código",
    "ug_responsavel_nome": "UG Responsável Nome",
    "pi_codigo": "PI Código PI",
    "pi_nome": "PI Nome",
    "natureza_despesa_codigo": "Natureza Despesa Código",
    "natureza_despesa_nome": "Métrica",  # the header cell; the column holds the name
}
TESOURO_ACAO_METRICS = {
    "9": "dotacao_inicial",
    "13": "dotacao_atualizada",
    "29": "despesas_empenhadas",
    "30": "despesas_empenhadas_a_liquidar",
    "32": "despesas_liquidadas_a_pagar",
    "34": "despesas_pagas",
    "50": "restos_a_pagar_inscritos",
    "52": "restos_a_pagar_pagos",
    "55": "liquidacoes_totais",
    "56": "pagamentos_totais",
}
TESOURO_ACAO_COLUMNS = [*TESOURO_ACAO_DIMENSIONS, *TESOURO_ACAO_METRICS.values()]


@parser("tesouro_orcamento_acao_zip")
def tesouro_orcamento_acao_zip(path: str) -> "pd.DataFrame":
    """ "Orçamento MCid por ação (com data)", e-mailed as a ZIP: one UTF-16 TSV.

    Title and filter rows, then a 3-row header: the dimension names, the Item Informação
    codes and their names. The header is checked before reading by position, so a report
    edited in the Tesouro Gerencial fails instead of shifting columns. Everything as text:
    values stay as the report gives them (`4.929.655,57`, `(200,00)`), and `000/2026` in
    `dia_lancamento` is the opening of the year, with no day.
    """
    import csv
    import io
    import zipfile

    import pandas as pd

    with zipfile.ZipFile(path) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise SourceLayoutChanged(f"ZIP without a CSV: {archive.namelist()}")
        rows = list(
            csv.reader(io.StringIO(archive.read(members[0]).decode("utf-16")), delimiter="\t")
        )
    start = next((i for i, r in enumerate(rows) if r[:1] == ["Dia Lançamento"]), None)
    if start is None:
        raise SourceLayoutChanged("no header row starting with 'Dia Lançamento'")
    n = len(TESOURO_ACAO_DIMENSIONS)
    names, codes = rows[start][:n], [c.strip() for c in rows[start + 1][n:]]
    if names != list(TESOURO_ACAO_DIMENSIONS.values()) or codes != list(TESOURO_ACAO_METRICS):
        raise SourceLayoutChanged(f"header changed: {names} / {codes}")
    data = [r for r in rows[start + 3 :] if any(c.strip() for c in r)]
    width = {len(r) for r in data}
    if width - {len(TESOURO_ACAO_COLUMNS)}:
        raise SourceLayoutChanged(f"expected {len(TESOURO_ACAO_COLUMNS)} columns, got {width}")
    return pd.DataFrame(data, columns=TESOURO_ACAO_COLUMNS, dtype=str)


def convert(
    minio,
    key: str,
    execution_id: str,
    apply: bool,
    converted: set,
    reader_name: Optional[str] = None,
    **options,
) -> list:
    """Staging records of one approved raw object, through the reader of its contract."""
    if (reader_name or "auto") not in READERS:
        raise KeyError(
            f"no reader {reader_name!r} for {key}; register it in plugins.datalake.readers"
        )
    return READERS[reader_name or "auto"](minio, key, execution_id, apply, converted, **options)
