"""staging -> analytics: métricas de cada arquivo e a seleção (fora vazios, o modelo de 2009 e cópias)."""

import hashlib
import re
from pathlib import Path
from typing import Dict, Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq

SELECIONADO, COPIA, VAZIO, MODELO_2009 = "selecionado", "copia", "vazio", "modelo_2009_sem_dado"

# cabeçalho do modelo de abr-jun/2009 que o dump gravou no lugar de 273 tabelas
COLUNAS_MODELO_2009 = frozenset(
    {"unnamed_0", "20090401_000000", "20090501_000000", "20090601_000000"}
)
_MESES = ("jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez")
_P_ANO_MES = re.compile(r"(?<!\d)((?:19|20)\d{2})_?(0[1-9]|1[0-2])(?!\d)")
_P_MES_ANO = re.compile(r"(?<![a-z])(" + "|".join(_MESES) + r")[a-zç]*_?((?:19|20)?\d{2})(?!\d)")
_P_ANO_MES_DIA = re.compile(r"(?<!\d)((?:19|20)\d{2})(0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])(?!\d)")
_P_DIA_MES_ANO = re.compile(
    r"(?<!\d)(?:0[1-9]|[12]\d|3[01])_?(0[1-9]|1[0-2])_?((?:19|20)?\d{2})(?!\d)"
)
_P_ANO = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
SCHEMA_ANALYTICS = pa.schema(
    [
        ("arquivo", pa.string()),
        ("staging_key", pa.string()),
        ("staging_etag", pa.string()),
        ("analytics_key", pa.string()),
        ("status", pa.string()),
        ("copia_de", pa.string()),
        ("periodo", pa.string()),
        ("n_linhas", pa.int64()),
        ("n_colunas", pa.int64()),
        ("celulas_preenchidas", pa.int64()),
        ("layout", pa.string()),
        ("colunas", pa.list_(pa.string())),
        ("hash_conteudo", pa.string()),
        ("gerado_em", pa.string()),
    ]
)


def periodo(nome: str) -> str:
    """Ano-mês do nome ("AAAAMM"; só o ano, "AAAA00"; nada, ""). Serve para escolher, entre
    cópias, a mais antiga."""
    m = _P_ANO_MES.search(nome)
    if m:
        return m.group(1) + m.group(2)
    m = _P_MES_ANO.search(nome)
    if m:
        ano = m.group(2) if len(m.group(2)) == 4 else "20" + m.group(2)
        return f"{ano}{_MESES.index(m.group(1)) + 1:02d}"
    m = _P_ANO_MES_DIA.search(nome)
    if m:
        return m.group(1) + m.group(2)
    m = _P_DIA_MES_ANO.search(nome)
    if m:
        ano = m.group(2) if len(m.group(2)) == 4 else "20" + m.group(2)
        return ano + m.group(1)
    m = _P_ANO.search(nome)
    return m.group(1) + "00" if m else ""


def _q(coluna: str) -> str:
    return '"' + coluna.replace('"', '""') + '"'


METRICAS_ANALYTICS = (
    "n_linhas",
    "n_colunas",
    "celulas_preenchidas",
    "layout",
    "colunas",
    "hash_conteudo",
)


def metricas_parquet(path: Path, con=None) -> dict:
    """Linhas, colunas de dado (sem as `_*`), células preenchidas e hash do conteúdo (md5,
    independente da ordem das linhas e dos nomes das colunas)."""
    import duckdb

    con = con or duckdb.connect()
    fonte = f"read_parquet('{path}')"
    colunas = [c for c in pq.read_schema(path).names if not c.startswith("_")]
    n_linhas = con.execute(f"select count(*) from {fonte}").fetchone()[0]
    r = {
        "n_linhas": n_linhas,
        "n_colunas": len(colunas),
        "colunas": colunas,
        "layout": hashlib.md5("|".join(sorted(colunas)).encode()).hexdigest()[:8],
        "celulas_preenchidas": 0,
        "hash_conteudo": None,
    }
    if not colunas or not n_linhas:
        return r
    cheias = " + ".join(f"count(nullif(trim({_q(c)}), ''))" for c in colunas)
    linha = ", ".join(f"coalesce({_q(c)}, chr(0))" for c in colunas)
    r["celulas_preenchidas"], r["hash_conteudo"] = con.execute(
        f"""select (select {cheias} from {fonte}),
                   (select md5(string_agg(h, ',' order by h))
                    from (select md5(concat_ws(chr(31), {linha})) as h from {fonte}))"""
    ).fetchone()
    return r


def classificar(registros: Iterable[dict]) -> List[dict]:
    """`status` e `copia_de`. Entre conteúdos idênticos fica o de menor (período, nome)."""
    regs = [dict(r) for r in registros]
    representante: Dict[str, str] = {}
    for r in sorted(regs, key=lambda r: (r["periodo"] or "999999", r["arquivo"])):
        r["copia_de"] = None
        if not r["n_colunas"] or not r["n_linhas"] or not r["celulas_preenchidas"]:
            r["status"] = VAZIO
        elif set(r["colunas"]) == COLUNAS_MODELO_2009:
            r["status"] = MODELO_2009
        elif r["hash_conteudo"] in representante:
            r["status"], r["copia_de"] = COPIA, representante[r["hash_conteudo"]]
        else:
            r["status"] = SELECIONADO
            representante[r["hash_conteudo"]] = r["arquivo"]
    return sorted(regs, key=lambda r: r["arquivo"])
