"""analytics -> cleansed, datas: competência e data de corte de cada saída."""

import re
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from historico_mcmv.analytics import _MESES

# 3b. datas: competência (mês a que o relatório se refere) e data de corte (extração)
# O nome do arquivo não basta: é `<pasta>_<nome original>`, a pasta nem sempre é o mês do
# dado e nomes com mais de 63 caracteres perdem a data do fim. Regras, nesta ordem:
# 1. `data_de_movimento` constante (SNH) é a competência e o corte;
# 2. lote de entrega do BB ao MCid (pf_pf, pj e pj_pf do mesmo dia, 2014-2019): mês do
#    contrato mais novo do pf_pf, ou o anterior se ele cai até o dia 12 (o `dat_atualizacao`
#    desses arquivos não é confiável);
# 3. data no nome original (a última com dia, senão a última com mês);
# 4. só o mês da pasta.
# O corte nunca fica antes do contrato mais novo. Conflitos viram alertas, não mudam nada.

COLUNAS_CONTRATO = (
    "dat_contratacao",
    "data_da_contratacao_bb",
    "data_de_contratacao",
    "data_contratacao",
    "dt_contratacao",
    "data_de_contratacao_convenio",
    "dte_assin_contrato",
    "dte_assin_contrat",  # nome cortado em 63 caracteres
)
COLUNAS_DECLARADAS = ("data_de_movimento", "dat_atualizacao", "data_pos_arq")
DECLARADA_CONFIAVEL = "data_de_movimento"
INICIO = date(2008, 1, 1)
DIA_LIMITE_FECHAMENTO = 12
DIA_FIM_DO_MES = 26
DIAS_CONTRATO_ANTIGO = 60
FONTE_MOVIMENTO, FONTE_LOTE = "data_de_movimento", "lote_contrato_mais_novo"
FONTE_NOME, FONTE_PASTA, FONTE_CONTRATO = "nome_arquivo", "pasta", "contrato_mais_novo"
_NOMES_MES = (
    "janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro"
)
_PASTA = re.compile(rf"(?:^|_)((?:19|20)\d{{2}})_(0[1-9]|1[0-2])_+(?:{_NOMES_MES})_+(.*)$")
_A4 = r"(?P<a>(?:19|20)\d{2})"
_A2 = r"(?P<a>(?:19|20)?\d{2})"
_M = r"(?P<m>0[1-9]|1[0-2])"
_D = r"(?P<d>0[1-9]|[12]\d|3[01])"
_MX = r"(?P<mx>" + "|".join(_MESES) + r")[a-zç]*"
_PADROES_DATA = (
    (re.compile(rf"(?<!\d){_A4}{_M}{_D}(?!\d)"), "dia"),  # 20131105
    (re.compile(rf"(?<!\d){_D}{_M}{_A4}(?!\d)"), "dia"),  # 31122016
    (re.compile(rf"(?<!\d){_A4}_{_M}_{_D}(?!\d)"), "dia"),  # 2012_04_18
    (re.compile(rf"(?<!\d){_D}_{_M}_{_A2}(?!\d)"), "dia"),  # 08_05_2012, 31_07_12
    (re.compile(rf"(?<!\d){_D}_?{_MX}_?{_A2}(?!\d)"), "dia"),  # 18abr11, 03_jan_12
    (re.compile(rf"(?<!\d){_D}{_M}(?P<a>0[89]|1\d|2[0-6])(?!\d)"), "dia"),  # 150513
    (re.compile(rf"(?<![a-z\d]){_MX}_?{_A2}(?!\d)"), "mes"),  # fev2017, mai18
    (re.compile(rf"(?<!\d){_A4}_?{_M}(?!\d)"), "mes"),  # 202411, 2024_10
)
_CORTADO = re.compile(rf"(?<!\d){_D}{_M}(?P<a3>(?:19|20)\d)$")  # "1610201" no fim do nome
_DIA_MES = re.compile(rf"(?<!\d){_D}{_M}(?!\d)")  # "3011", com o ano da pasta
_DIAS_DA_PASTA = 62
_LOTE_MIN_CIDADES = re.compile(r"^(.+?)_min_cidades_(pf_pf|pj_pf|pj)(?:_\d{4}_\d{2})?$")
_LOTE_BB = re.compile(r"^(bb_20(?:18|19)_.+?)_(pf_pf|pj_pf|pj)$")
_PREFERENCIA_LOTE = ("pf_pf", "pj_pf", "pj")


def _data(ano, mes, dia) -> Optional[date]:
    ano = int(ano)
    try:
        return date(ano + 2000 if ano < 100 else ano, int(mes), int(dia))
    except ValueError:
        return None


def _mes(d: Optional[date]) -> Optional[str]:
    return f"{d.year:04d}-{d.month:02d}" if d else None


def _mes_anterior(d: date) -> str:
    return _mes(d.replace(day=1) - timedelta(days=1))


def _datas_completas(texto: str, hoje: date) -> List[tuple]:
    achadas = []
    for rx, precisao in _PADROES_DATA:
        for a in rx.finditer(texto):
            g = a.groupdict()
            mes = _MESES.index(g["mx"]) + 1 if g.get("mx") else g["m"]
            d = _data(g["a"], mes, g.get("d") or 1)
            if d and INICIO <= d <= hoje:
                achadas.append((a.start(), a.end(), d, precisao))
    return achadas


def _datas_com_ano_da_pasta(texto: str, pasta: date, so_cortadas: bool) -> List[tuple]:
    achadas = []
    for rx in (_CORTADO,) if so_cortadas else (_CORTADO, _DIA_MES):
        for a in rx.finditer(texto):
            if rx is _CORTADO and not str(pasta.year).startswith(a.group("a3")):
                continue
            d = _data(pasta.year, a.group("m"), a.group("d"))
            if d and abs((d - pasta).days) <= _DIAS_DA_PASTA:
                achadas.append((a.start(), a.end(), d, "dia"))
    return achadas


def data_do_nome(nome: str, hoje: Optional[date] = None) -> Tuple[Optional[date], Optional[str]]:
    """(data, precisão "dia" | "mes") do nome original; sem data nele, o mês da pasta com
    precisão "pasta". Vale a última data com dia, senão a última com mês."""
    hoje = hoje or date.today()
    pasta, original = None, nome
    m = _PASTA.search(nome)
    if m:
        pasta, original = date(int(m.group(1)), int(m.group(2)), 1), m.group(3)
    achadas = _datas_completas(original, hoje)
    if pasta:
        achadas += _datas_com_ano_da_pasta(original, pasta, so_cortadas=bool(achadas))
    achadas.sort(key=lambda x: (x[0], x[0] - x[1]))
    finais = []
    for a in achadas:  # um achado dentro de outro maior não conta
        if not any(b[0] <= a[0] and a[1] <= b[1] for b in finais):
            finais.append(a)
    if finais:
        com_dia = [a for a in finais if a[3] == "dia"]
        escolhida = max(com_dia or finais, key=lambda x: x[0])
        return escolhida[2], escolhida[3]
    return (pasta, FONTE_PASTA) if pasta else (None, None)


def mes_da_pasta(nome: str) -> Optional[str]:
    m = _PASTA.search(nome)
    return f"{m.group(1)}-{m.group(2)}" if m else None


def para_datas(valores: Iterable) -> pd.Series:
    """AAAA-MM-DD ou DD/MM/AAAA (com ou sem hora) -> datetime; o resto vira NaT."""
    s = pd.Series(list(valores), dtype="string")
    iso = s.str.extract(r"^\s*(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})")
    br = s.str.extract(r"^\s*(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})")
    partes = iso.fillna(br).astype("float")
    if partes.empty:
        return pd.Series([], dtype="datetime64[ns]")
    validas = partes.notna().all(axis=1)
    saida = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if validas.any():
        saida[validas] = pd.to_datetime(partes[validas], errors="coerce")
    return saida


def evidencias(df: pd.DataFrame, hoje: Optional[date] = None) -> dict:
    """O que o conteúdo diz sobre a data: o contrato mais novo e a data declarada."""
    hoje = hoje or date.today()
    ev = {
        "contrato_coluna": None,
        "contrato_mais_novo": None,
        "data_declarada_coluna": None,
        "data_declarada": None,
        "data_declarada_distintas": 0,
    }
    coluna = next((c for c in COLUNAS_CONTRATO if c in df.columns), None)
    if coluna:
        datas = para_datas(df[coluna].dropna().unique())
        datas = datas[(datas >= pd.Timestamp(INICIO)) & (datas <= pd.Timestamp(hoje))]
        ev["contrato_coluna"] = coluna
        ev["contrato_mais_novo"] = datas.max().date() if len(datas) else None
    coluna = next((c for c in COLUNAS_DECLARADAS if c in df.columns), None)
    if coluna:
        valores = sorted({str(v).strip() for v in df[coluna].dropna().unique()} - {""})
        ev["data_declarada_coluna"] = coluna
        ev["data_declarada_distintas"] = len(valores)
        ev["data_declarada"] = valores[0] if len(valores) == 1 else None
    return ev


def lote(arquivo: str) -> Tuple[Optional[str], Optional[str]]:
    """(lote, sub-arquivo) de um arquivo da entrega do BB ao MCid; fora dela, (None, None)."""
    m = _LOTE_MIN_CIDADES.match(arquivo) or _LOTE_BB.match(arquivo)
    if not m:
        return None, None
    return re.sub(r"(\d{4}_\d{2}_\d{2})_\1$", r"\1", m.group(1)), m.group(2)


def _referencia_do_lote(membros: List[Tuple[str, dict]]) -> Optional[Tuple[date, str, date]]:
    for preferido in _PREFERENCIA_LOTE:
        datas = [r["contrato_mais_novo"] for sub, r in membros if sub == preferido]
        datas = [d for d in datas if d]
        if datas:
            todas = [r["contrato_mais_novo"] for _, r in membros if r["contrato_mais_novo"]]
            return max(datas), preferido, max(todas)
    return None


def datar(registros: List[dict], hoje: Optional[date] = None) -> None:
    """Competência, corte, fontes e alertas em cada registro do log do cleansed."""
    hoje = hoje or date.today()
    for r in registros:
        for c in ("contrato_mais_novo", "data_declarada", "data_declarada_coluna"):
            r.setdefault(c, None)
    lotes: Dict[str, List[Tuple[str, dict]]] = {}
    for r in registros:
        r["lote"], sub = lote(r["arquivo"])
        if r["lote"]:
            lotes.setdefault(r["lote"], []).append((sub, r))
    referencias = {nome: _referencia_do_lote(membros) for nome, membros in lotes.items()}
    for r in registros:
        _datar_um(r, referencias.get(r["lote"]), hoje)


def _data_declarada(r: dict, alertas: List[str]) -> Optional[date]:
    if r.get("data_declarada_distintas", 0) > 1:
        alertas.append("data_declarada_varia")
    if not r["data_declarada"]:
        return None
    d = para_datas([r["data_declarada"]]).iloc[0]
    if pd.isna(d):
        alertas.append("data_declarada_invalida")
        return None
    return d.date()


def _competencia_do_lote(referencia_lote: tuple, alertas: List[str]) -> str:
    referencia, sub, _ = referencia_lote
    if sub != _PREFERENCIA_LOTE[0]:
        alertas.append("lote_sem_pf_pf")
    if referencia.day <= DIA_LIMITE_FECHAMENTO:
        return _mes_anterior(referencia)
    if referencia.day < DIA_FIM_DO_MES:
        alertas.append("corte_no_meio_do_mes")
    return _mes(referencia)


def _alertas_da_competencia(r: dict, competencia, fonte, declarada, alertas) -> None:
    if competencia is None:
        alertas.append("sem_competencia")
        return
    if fonte in (FONTE_MOVIMENTO, FONTE_LOTE) and r["data_nome"]:
        if competencia != _mes(r["data_nome"]):
            alertas.append("competencia_diferente_do_nome")
    inicio = date(int(competencia[:4]), int(competencia[5:]), 1)
    contrato = r["contrato_mais_novo"]
    if contrato and (inicio - contrato).days > DIAS_CONTRATO_ANTIGO:
        alertas.append("contrato_mais_novo_antigo")
    if declarada and fonte != FONTE_MOVIMENTO and _mes(declarada) != competencia:
        alertas.append("data_declarada_diferente")


def _datar_um(r: dict, referencia_lote: Optional[Tuple[date, str, date]], hoje: date) -> None:
    alertas: List[str] = []
    data_nome, precisao = data_do_nome(r["arquivo"], hoje)
    r["data_nome"], r["data_nome_precisao"] = data_nome, precisao
    pasta = mes_da_pasta(r["arquivo"])
    if pasta and precisao != FONTE_PASTA and data_nome and _mes(data_nome) != pasta:
        alertas.append("pasta_diferente_do_nome")
    declarada = _data_declarada(r, alertas)
    competencia = fonte = corte = fonte_corte = None
    contrato = r["contrato_mais_novo"]
    if declarada and r["data_declarada_coluna"] == DECLARADA_CONFIAVEL:
        competencia, fonte = _mes(declarada), FONTE_MOVIMENTO
        corte, fonte_corte = declarada, FONTE_MOVIMENTO
    elif referencia_lote:
        competencia, fonte = _competencia_do_lote(referencia_lote, alertas), FONTE_LOTE
        contrato = referencia_lote[2]  # o lote tem um corte só
    elif data_nome:
        competencia = _mes(data_nome)
        fonte = FONTE_PASTA if precisao == FONTE_PASTA else FONTE_NOME
    _alertas_da_competencia(r, competencia, fonte, declarada, alertas)
    if corte is None and precisao == "dia":
        corte, fonte_corte = data_nome, FONTE_NOME
    if corte and contrato and contrato > corte:
        alertas.append("contrato_depois_do_corte")
        corte, fonte_corte = contrato, FONTE_CONTRATO
    r.update(
        competencia=competencia,
        competencia_fonte=fonte,
        data_corte=corte,
        data_corte_fonte=fonte_corte,
        alertas_data=alertas,
    )
