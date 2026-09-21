"""Política de nulos/duplicados ciente de APF.

Módulo novo (OpenSpec change ``padronizacao-dominio-nulos-duplicados``) com:

* **Classificação de nulos:** ``classificar_nulos`` categoriza cada valor da
  série em ``nulo_legitimo`` / ``placeholder`` / ``nulo_a_preencher`` /
  ``preenchido``.
* **Conversão de placeholders:** ``converter_placeholders`` substitui
  placeholders por ``pd.NA``.
* **Deduplicação SEMÂNTICA ciente de APF:** ``deduplicar_ciente_apf`` remove
  duplicatas EXATAS sobre a chave composta
  ``(chave_negocio + colunas_fase + coluna_snapshot)`` — **nunca** sobre
  ``apf`` isolado. Um mesmo APF em fases/snapshots distintos é um dado
  legítimo de série temporal e deve ser preservado.

A dedup por hash MD5 de conteúdo (byte-a-byte) já existe em
``deduplicacao.py`` e é separada desta dedup semântica.

Todas as funções são puras (sem I/O).
"""

from __future__ import annotations

import re
import unicodedata
from typing import cast

import pandas as pd

# Placeholders globais: valores que representam "ausência de dado" e devem
# ser tratados como nulos. A lista é determinística e documentada.
_PLACEHOLDERS_GLOBAIS: frozenset[str] = frozenset({"", "-", "NULL", "null", "1900-01-01"})

# Para colunas id/data-like, zeros também são placeholders (ex.: código IBGE
# vazio gravado como "0", data zerada como "0,00").
_PLACEHOLDERS_ID_DATA: frozenset[str] = frozenset({"0", "0,00"})

_CATEGORIAS_NULOS: tuple[str, ...] = (
    "nulo_legitimo",
    "placeholder",
    "nulo_a_preencher",
    "preenchido",
)


def _normalizar_nome_coluna(nome: str) -> str:
    """Normaliza nome de coluna (NFKD, ASCII, lowercase, underscores)."""
    texto = unicodedata.normalize("NFKD", str(nome))
    texto = texto.encode("ascii", "ignore").decode("ascii").lower()
    texto = re.sub(r"[^a-z0-9_]", "_", texto)
    texto = re.sub(r"_+", "_", texto).strip("_")
    return texto or "col"


def _eh_coluna_id_data(nome: str) -> bool:
    """True se o nome normalizado sugere coluna de ID ou data.

    IDs: prefixos ``cod``, ``nu_``, ``nr_``, ``co_`` ou token ``id``.
    Datas: prefixos ``dt``, ``dat``, ``data``.
    """
    nome_norm = _normalizar_nome_coluna(nome)
    if nome_norm.startswith(("cod", "nu_", "nr_", "co_", "dt", "dat", "data")):
        return True
    if any(token == "id" for token in nome_norm.split("_")):
        return True
    return False


def _eh_nulo(valor: object) -> bool:
    if valor is None or valor is pd.NA or valor is pd.NaT:
        return True
    if isinstance(valor, float):
        return float(valor) != float(valor)  # NaN
    return False


def _eh_placeholder(valor: object, id_data: bool) -> bool:
    """True se o valor é um placeholder (vazio/-/NULL/1900-01-01 e, em
    colunas id/data-like, também zeros)."""
    if _eh_nulo(valor):
        return False
    if isinstance(valor, str):
        texto = valor.strip()
        placeholders = _PLACEHOLDERS_GLOBAIS
        if id_data:
            placeholders = placeholders | _PLACEHOLDERS_ID_DATA
        return texto in placeholders or texto.lower() in placeholders
    if isinstance(valor, (int, float)) and id_data:
        return float(valor) == 0.0
    return False


def classificar_nulos(series: pd.Series) -> pd.Series:
    """Classifica cada valor da série em uma das 4 categorias.

    Regras (determinísticas e documentadas):

    * ``preenchido`` — valor presente e não-placeholder.
    * ``placeholder`` — valor vazio/``"-"``/``"NULL"``/``"null"``/
      ``"1900-01-01"`` (globais) e, em colunas id/data-like (nome normalizado
      com prefixo ``cod``/``nu_``/``nr_``/``co_``/``dt``/``dat``/``data`` ou
      token ``id``), também ``"0"``/``"0,00"`` e zeros numéricos.
    * ``nulo_legitimo`` — valor nulo (``NaN``/``None``/``pd.NA``) em coluna
      não-chave (ausência aceitável).
    * ``nulo_a_preencher`` — valor nulo em coluna id/data-like (chave ou data
      obrigatória que deveria estar preenchida).

    Parameters
    ----------
    series : pd.Series
        Série (de preferência com ``series.name`` definido para detectar
        colunas id/data-like).

    Returns
    -------
    pd.Series
        Série de strings ∈ {nulo_legitimo, placeholder, nulo_a_preencher,
        preenchido}, com o mesmo índice.
    """
    id_data = _eh_coluna_id_data(str(series.name)) if series.name is not None else False

    def _classificar(valor: object) -> str:
        if _eh_nulo(valor):
            return "nulo_a_preencher" if id_data else "nulo_legitimo"
        if _eh_placeholder(valor, id_data):
            return "placeholder"
        return "preenchido"

    return series.map(_classificar).astype(str)


def converter_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui placeholders por ``pd.NA`` (nulos padronizados).

    Aplica as mesmas regras de ``classificar_nulos``:
    * colunas object: vazio/``"-"``/``"NULL"``/``"null"``/``"1900-01-01"``
      e, em id/data-like, ``"0"``/``"0,00"``/zeros numéricos → ``pd.NA``;
    * colunas numéricas id/data-like: zero numérico → ``pd.NA``.

    Não altera colunas datetime (valores zerados já viram ``NaT`` no parse).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a limpar.

    Returns
    -------
    pd.DataFrame
        Cópia com placeholders convertidos a ``pd.NA``.
    """
    df = df.copy()

    for col in df.columns:
        id_data = _eh_coluna_id_data(str(col))
        dtype = df[col].dtype

        if pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
            serie_col = cast(pd.Series, df[col])
            mask = serie_col.map(lambda v: _eh_placeholder(v, id_data))
            if bool(mask.any()):
                df.loc[mask, col] = pd.NA
        elif id_data and pd.api.types.is_numeric_dtype(dtype):
            df.loc[df[col] == 0, col] = pd.NA

    return df


def _eh_coluna_apf(nome: str) -> bool:
    """True se o nome normalizado da coluna é de APF (contém ``apf``)."""
    return "apf" in _normalizar_nome_coluna(nome)


def deduplicar_ciente_apf(
    df: pd.DataFrame,
    chave_negocio: list[str],
    colunas_fase: list[str],
    coluna_snapshot: str | None,
) -> pd.DataFrame:
    """Deduplica de forma ciente de APF: remove duplicatas EXATAS sobre a
    chave composta ``(chave_negocio + colunas_fase + coluna_snapshot)``.

    Um mesmo APF registrado em fases distintas (ex.: ``PROJETO`` vs ``OBRA``)
    ou em snapshots temporais distintos é dado legítimo de série temporal e é
    **preservado**. Apenas linhas idênticas na chave composta são removidas
    (mantém a primeira).

    **Validação:** ``apf`` nunca pode ser usado isoladamente como chave — se
    a chave composta resultar em exatamente ``{"apf"}``, levanta
    ``ValueError``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame de entrada.
    chave_negocio : list[str]
        Colunas da chave de negócio (deve incluir ``apf``; nunca só ele).
    colunas_fase : list[str]
        Colunas de fase/estágio (ex.: ``fase_contrato``, ``situacao_obra``).
    coluna_snapshot : str or None
        Coluna de snapshot temporal (ex.: ``dt_movimento``) ou ``None``.

    Returns
    -------
    pd.DataFrame
        Cópia deduplicada (``reset_index``), sem alterar a entrada.

    Raises
    ------
    ValueError
        Se ``chave_negocio`` for vazia ou se a chave composta for apenas
        ``{"apf"}``.
    KeyError
        Se alguma coluna referenciada não existir no DataFrame.
    """
    if not chave_negocio:
        raise ValueError("chave_negocio não pode ser vazio")

    df = df.copy()

    def _validar(cols: list[str], rotulo: str) -> list[str]:
        presentes = [c for c in cols if c in df.columns]
        ausentes = [c for c in cols if c not in df.columns]
        if ausentes:
            raise KeyError(f"Colunas {rotulo} ausentes do DataFrame: {ausentes}")
        return presentes

    chave_ok = _validar(chave_negocio, "da chave de negócio")
    fase_ok = _validar(colunas_fase, "de fase")
    snapshot_ok = [coluna_snapshot] if coluna_snapshot else []
    if coluna_snapshot and coluna_snapshot not in df.columns:
        raise KeyError(f"Coluna de snapshot ausente do DataFrame: {coluna_snapshot}")

    chave_dedup = set(chave_ok) | set(fase_ok) | set(snapshot_ok)
    if chave_dedup and all(_eh_coluna_apf(c) for c in chave_dedup):
        raise ValueError(
            "'apf' não pode ser usado isoladamente como chave de "
            "deduplicação — informe colunas de fase e/ou snapshot"
        )

    subset = list(dict.fromkeys(chave_ok + fase_ok + snapshot_ok))
    return df.drop_duplicates(subset=subset, keep="first").reset_index(drop=True)
