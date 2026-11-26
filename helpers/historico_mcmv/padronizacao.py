"""analytics -> cleansed, padronização: um jeito só de escrever cada valor, sem mudar o que ele diz."""

import hashlib
import logging
import re
from datetime import date
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from historico_mcmv.erros import ErroHistorico
from historico_mcmv.estrutura import DUPLICADA, LINHA_ORIGEM, _cheias, _descartes
from historico_mcmv.nomes import corrigir_mojibake as _corrigir_mojibake

log = logging.getLogger(__name__)


# 3a. padronização: um jeito só de escrever cada valor, sem mudar o que ele diz
# Continua tudo texto. Por coluna de cada arquivo: tira o espaço das pontas, conserta o
# mojibake e esvazia os nulos escritos; se a coluna é de data, escreve AAAA-MM-DD; se é de
# número, ponto decimal, sem milhar, sem ".0", sem zero à direita e sem ruído de float.
# Toda célula alterada é conferida (o número e a data continuam os mesmos) e contada; o
# texto original continua no analytics.

ESPACO, MOJIBAKE, NULO, MOEDA = "espaco", "mojibake", "nulo", "moeda"
DATA, DATA_INVALIDA = "data", "data_invalida"
PONTO_ZERO, DECIMAL_BR, RUIDO_FLOAT, FORMA_NUMERO = (
    "numero_ponto_zero",
    "numero_decimal_br",
    "numero_ruido_float",
    "numero_forma",
)
CNPJ_ZEROS = "cnpj_zeros"
_NULOS = frozenset(
    {"null", "nan", "none", "nat", "#n/d", "#n/a", "n/d", "-", "--", "---", "—", "–"}
    | {"0000-00-00", "00/00/0000", "0000-00-00 00:00:00", "00:00:00", "00:00:00.000"}
)
_MOJIBAKE_RX = re.compile("Ã[\x80-\xbf]|Â[\x80-\xbf]|â€")
_D_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?$")
_D_BR = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})(?: (\d{2}):(\d{2})(?::(\d{2}))?)?$")
_N_INT = re.compile(r"^-?\d+$")
_N_PONTO = re.compile(r"^-?\d+\.\d+$")
_N_MILHAR_BR = re.compile(r"^-?\d{1,3}(\.\d{3})+$")  # "1.000": milhar no BR, decimal no resto
_N_BR = re.compile(r"^-?\d{1,3}(\.\d{3})*,\d+$|^-?\d+,\d+$")

# "1.737.972,88": ponto de milhar e vírgula decimal juntos só existem no BR, então vale até
# em coluna que mistura as convenções (SNH BB de 2024-06)
_N_BR_MILHAR = re.compile(r"^-?\d{1,3}(\.\d{3})+,\d+$")
_MOEDA = re.compile(r"^R\$\s*(-|-?[\d.,]+)$")  # "R$ 1.008.000,00", "R$ -"
_N_CIENT = re.compile(r"^-?\d+(\.\d+)?[eE][+-]?\d+$")

# ",05": vírgula decimal sem a parte inteira. Não pode ser milhar, então é decimal em
# qualquer convenção (aparece até em coluna de ponto decimal, como no INT040)
_N_FRACAO = re.compile(r"^-?,\d+$")
_CNPJ_COLUNA = re.compile(r"cnpj")
LIMIAR_TIPO = 0.95  # fração dos valores para a coluna ser de data ou de número
CASAS_RUIDO = 7  # decimal com tantas casas ou mais é ruído de float


def _limpar_texto(v: str) -> Tuple[str, Optional[str]]:
    t = v.replace("\xa0", " ").strip()
    regra = ESPACO if t != v else None
    moeda = _MOEDA.match(t)
    if moeda:
        t, regra = moeda.group(1), MOEDA
    if _MOJIBAKE_RX.search(t):
        corrigido = _corrigir_mojibake(t)
        if corrigido != t:
            t, regra = corrigido, MOJIBAKE
    if t and t.lower() in _NULOS:
        return "", NULO
    return t, regra


def _e_data(t: str) -> bool:
    return bool(_D_ISO.match(t) or _D_BR.match(t))


def _e_numero(t: str) -> bool:
    return any(rx.match(t) for rx in (_N_INT, _N_PONTO, _N_MILHAR_BR, _N_BR, _N_CIENT, _N_FRACAO))


def _data_iso(t: str) -> Tuple[str, str]:
    """(valor, regra) de uma data: AAAA-MM-DD (com a hora só se não for 00:00:00)."""
    m = _D_ISO.match(t)
    if m:
        a, mes, d, hh, mm, ss = m.groups()
    else:
        d, mes, a, hh, mm, ss = _D_BR.match(t).groups()
    try:
        dia = date(int(a), int(mes), int(d))
    except ValueError:
        return "", DATA_INVALIDA
    hora = f"{hh}:{mm}:{ss or '00'}" if hh and f"{hh}{mm}{ss or '00'}" != "000000" else ""
    novo = dia.isoformat() + (f" {hora}" if hora else "")
    return novo, (DATA if novo != t else None)


def _convencao(valores: Iterable[str]) -> Optional[str]:
    """ "br" (vírgula decimal), "ponto", "inteiro" (só inteiros e "1.000", que não dá para
    saber se é mil ou um) ou None quando a coluna mistura vírgula e ponto decimal."""
    br = ponto = False
    for t in valores:
        br = br or bool(_N_BR.match(t))
        ponto = ponto or bool(_N_PONTO.match(t) and not _N_MILHAR_BR.match(t))
    if br and ponto:
        return None
    return "br" if br else ("ponto" if ponto else "inteiro")


def _sem_ruido(texto: str) -> str:
    """A forma curta de um decimal com ruído de float (float32 ou float64)."""
    import numpy as np

    x = float(texto)
    f32 = np.float32(x)
    if float(f32) == x:
        curto = np.format_float_positional(f32, unique=True, trim="-")
    else:
        curto = format(x, ".15g")
    return format(Decimal(curto).normalize(), "f")


def _numero_canonico(t: str, convencao: str) -> Tuple[str, Optional[str]]:
    """(valor, regra) de um número: ponto decimal, sem milhar, sem zero à direita."""
    regra = None
    s = t
    if _N_FRACAO.match(t):
        s, regra = t.replace(",", "."), DECIMAL_BR
    elif convencao == "br" and (_N_BR.match(t) or _N_MILHAR_BR.match(t)):
        s, regra = t.replace(".", "").replace(",", "."), DECIMAL_BR
    d = Decimal(s)
    if d == d.to_integral_value():
        novo = str(int(d))
        if regra is None and novo != t:
            regra = PONTO_ZERO if re.fullmatch(r"-?\d+\.0+", t) else FORMA_NUMERO
        return novo, regra
    casas = len(s.split(".")[1]) if "." in s and "e" not in s.lower() else 0
    if casas >= CASAS_RUIDO:
        novo = _sem_ruido(s)
        return novo, (RUIDO_FLOAT if novo != t else regra)
    novo = format(d.normalize(), "f")
    return novo, (regra or (FORMA_NUMERO if novo != t else None))


def _conferir_numero(antes: str, depois: str, convencao: str, coluna: str) -> None:
    s = antes.replace(".", "").replace(",", ".") if convencao == "br" and "," in antes else antes
    if convencao == "br" and _N_MILHAR_BR.match(antes):
        s = antes.replace(".", "")
    if _N_FRACAO.match(antes):
        s = antes.replace(",", ".")
    a, b = float(s), float(depois)
    if abs(a - b) > 1e-6 * max(1.0, abs(a)):
        raise ErroHistorico(f"padronização mudou o número em {coluna}: {antes!r} -> {depois!r}")


def _tipo_coluna(contagem: Dict[str, int]) -> str:
    cheias = {t: n for t, n in contagem.items() if t}
    total = sum(cheias.values())
    if not total:
        return "texto"
    if sum(n for t, n in cheias.items() if _e_data(t)) >= LIMIAR_TIPO * total:
        return "data"
    if sum(n for t, n in cheias.items() if _e_numero(t)) >= LIMIAR_TIPO * total:
        return "numero"
    return "texto"


def _mapa_numerico(coluna: str, limpos: Dict[str, str]) -> Dict[str, Tuple[str, Optional[str]]]:
    numeros = [t for t in set(limpos.values()) if t and _e_numero(t)]
    if any(re.match(r"^-?0\d", t) for t in numeros):
        # código com zero à esquerda (CEP, código): só sai o ".0"
        return {
            t: (re.sub(r"\.0+$", "", t), PONTO_ZERO)
            for t in numeros
            if re.fullmatch(r"-?\d+\.0+", t)
        }
    convencao = _convencao(numeros)
    if convencao is None:
        log.warning("%s mistura vírgula e ponto decimal: só o milhar BR inequívoco muda", coluna)
        mapa = {}
        for t in numeros:
            if _N_BR_MILHAR.match(t):
                novo, regra = _numero_canonico(t, "br")
                _conferir_numero(t, novo, "br", coluna)
                mapa[t] = (novo, regra)
        return mapa
    mapa = {}
    for t in numeros:
        if convencao == "inteiro" and _N_MILHAR_BR.match(t):
            continue  # ambíguo: fica como está
        novo, regra = _numero_canonico(t, convencao)
        if regra:
            _conferir_numero(t, novo, convencao, coluna)
            mapa[t] = (novo, regra)
    return mapa


def padronizar_coluna(coluna: str, serie: pd.Series) -> Tuple[pd.Series, Dict[str, int], Dict]:
    """(série padronizada, células por regra, exemplo antes -> depois por regra)."""
    unicos = serie.fillna("").value_counts().to_dict()
    passo1 = {v: _limpar_texto(v) for v in unicos}
    limpos = {v: t for v, (t, _) in passo1.items()}
    final = dict(passo1)
    tipo = _tipo_coluna(_somar(limpos, unicos))
    if tipo == "data":
        segundo = {t: _data_iso(t) for t in set(limpos.values()) if t and _e_data(t)}
    elif tipo == "numero":
        segundo = _mapa_numerico(coluna, limpos)
    else:
        segundo = {}
    for v, t in limpos.items():
        if t in segundo and segundo[t][1]:
            final[v] = segundo[t]
    if _CNPJ_COLUNA.search(coluna):
        for v, (t, _regra) in final.items():
            if re.fullmatch(r"\d{11,13}", t):
                final[v] = (t.zfill(14), CNPJ_ZEROS)
    contagem: Dict[str, int] = {}
    exemplos: Dict[str, Tuple[str, str]] = {}
    for v, (t, regra) in final.items():
        if regra and t != v:
            contagem[regra] = contagem.get(regra, 0) + unicos[v]
            exemplos.setdefault(regra, (v, t))
    mapa = {v: t for v, (t, regra) in final.items() if regra and t != v}
    return (serie.map(mapa).fillna(serie) if mapa else serie), contagem, exemplos


def _somar(limpos: Dict[str, str], unicos: Dict[str, int]) -> Dict[str, int]:
    soma: Dict[str, int] = {}
    for v, t in limpos.items():
        soma[t] = soma.get(t, 0) + unicos[v]
    return soma


def padronizar(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[dict]]:
    """Padroniza todas as colunas de dado. Devolve (dados, [{coluna, regra, celulas,
    antes, depois}]). As células preenchidas só diminuem pelos nulos e datas inválidas."""
    saida = df.copy()
    registros = []
    for c in [c for c in df.columns if c != LINHA_ORIGEM]:
        saida[c], contagem, exemplos = padronizar_coluna(c, df[c])
        for regra, n in contagem.items():
            antes, depois = exemplos[regra]
            registros.append(dict(coluna=c, regra=regra, celulas=n, antes=antes, depois=depois))
    esvaziadas = sum(r["celulas"] for r in registros if r["regra"] in (NULO, DATA_INVALIDA))
    if _cheias(saida) != _cheias(df) - esvaziadas:
        raise ErroHistorico(
            "padronização mudou células preenchidas além dos nulos e datas inválidas"
        )
    return saida, registros


def remover_duplicadas(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Linha idêntica repetida (fora `_linha_origem`): fica a primeira. Só vale em família
    com chave (APF, código, contrato), onde a repetição é registro duplicado na fonte."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    repetida = df.duplicated(cols, keep="first")
    return df[~repetida].reset_index(drop=True), _descartes(df[repetida], cols, DUPLICADA)


def hash_conteudo(df: pd.DataFrame) -> str:
    """md5 do conteúdo, independente da ordem das linhas (sem `_linha_origem`)."""
    cols = [c for c in df.columns if c != LINHA_ORIGEM]
    linhas = pd.util.hash_pandas_object(df[cols], index=False).sort_values()
    return hashlib.md5(("|".join(cols) + ":" + ",".join(map(str, linhas))).encode()).hexdigest()
