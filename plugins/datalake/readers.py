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
