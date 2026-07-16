"""Profiling estrutural de amostras CSV para classificação por formação.

Funções puras sobre um ``DataFrame`` já carregado (com ``index_col=0``, de modo
que ``df.columns`` contém apenas colunas de dados — "excluindo o índice pandas").

* ``profile_estrutural`` — métricas de shape, tamanho e razão de linhas vazias.
* ``classificar_coluna`` / ``classificar_colunas`` — heurística R3a (classifica
  cada coluna como ``UNNAMED``, ``DATA_VALUE`` ou ``REAL_NAME``).
* ``inspecionar_linhas_cabecalho`` — identifica linhas candidatas a cabeçalho
   nas primeiras N linhas de dados (subsídio para R6/R7).
* ``inspecionar_rodape`` — identifica linhas de metadados no final da tabela
   (subsídio para R6 ``cabecalho_composto_1``).
"""

from __future__ import annotations

import re
from typing import TypedDict

import pandas as pd

# Classificações de coluna (R3a)
UNNAMED = "UNNAMED"
DATA_VALUE = "DATA_VALUE"
REAL_NAME = "REAL_NAME"

# Padrões de nome
_UNNAMED_RE = re.compile(r"^unnamed_\d+$|^unnamed:\s*\d+$|^\s*$", re.IGNORECASE)
_DATE_RE = re.compile(r"^\d{8}$|^\d{8}_\d{6}$")
_TOKEN_RE = re.compile(r"^[^\s_]+$")  # token sem underscore nem espaço


class Profile(TypedDict):
    """Métricas estruturais de uma amostra."""

    n_rows: int
    n_cols: int
    n_data_cols: int
    file_size: int
    empty_row_ratio: float


class RowInfo(TypedDict):
    """Informação sobre uma das primeiras linhas de dados."""

    index: int
    n_non_empty: int
    n_text: int
    is_header: bool
    is_sparse: bool


class HeaderInspection(TypedDict):
    """Resultado da inspeção de linhas de cabeçalho para R6."""

    rows: list[RowInfo]
    n_empty_first_rows: int
    n_cols: int


def _is_numeric_value(s: object) -> bool:
    """True se o valor (string) pode ser interpretado como número.

    Aceita formatos brasileiros (vírgula decimal, ponto como separador de
    milhar) e ponto decimal. Datas e texto não são numéricos.
    """
    if not isinstance(s, str):
        return False
    text = s.strip()
    if not text:
        return False
    candidates = (
        text,
        text.replace(".", "").replace(",", "."),
        text.replace(",", "."),
    )
    for cand in candidates:
        try:
            float(cand)
            return True
        except ValueError:
            continue
    return False


def _is_text_value(s: object) -> bool:
    """True se o valor é texto não numérico com pelo menos uma letra."""
    if not isinstance(s, str):
        return False
    text = s.strip()
    if not text or _is_numeric_value(text):
        return False
    return any(ch.isalpha() for ch in text)


def profile_estrutural(df: pd.DataFrame, file_size: int = 0) -> Profile:
    """Retorna métricas estruturais da amostra.

    ``n_data_cols`` exclui o índice pandas — como o DataFrame é carregado com
    ``index_col=0``, ``df.columns`` já são apenas colunas de dados, logo
    ``n_data_cols == n_cols``.
    """
    n_rows = int(len(df))
    n_cols = int(len(df.columns))
    if n_rows > 0:
        n_empty = int(df.isna().all(axis=1).sum())
        empty_row_ratio = n_empty / n_rows
    else:
        empty_row_ratio = 0.0
    return Profile(
        n_rows=n_rows,
        n_cols=n_cols,
        n_data_cols=n_cols,
        file_size=file_size,
        empty_row_ratio=empty_row_ratio,
    )


def _is_unnamed(name: str) -> bool:
    return bool(_UNNAMED_RE.match(name.strip()))


def _is_date_name(name: str) -> bool:
    return bool(_DATE_RE.match(name.strip()))


def classificar_coluna(name: str, col_data: pd.Series) -> str:
    """Classifica uma coluna como ``UNNAMED``, ``DATA_VALUE`` ou ``REAL_NAME``.

    Heurística R3a (ordem de avaliação):
    1. Sem nome (``unnamed_N`` / ``Unnamed: N`` / vazio) → ``UNNAMED``.
    2. Padrão de data (``YYYYMMDD`` ou ``YYYYMMDD_HHMMSS``) → ``DATA_VALUE``.
    3. Nome puramente numérico E coluna >50% numérica → ``DATA_VALUE``.
    4. Token curto (≤20 chars, sem underscore/espaço) que aparece como valor
       nos primeiros 20 valores não nulos da coluna → ``DATA_VALUE``.
    5. Nome alfanumérico descritivo (contém letras) → ``REAL_NAME``.
    """
    name_str = str(name).strip()

    # 1. Sem nome
    if _is_unnamed(name_str):
        return UNNAMED

    # 2. Padrão de data como nome
    if _is_date_name(name_str):
        return DATA_VALUE

    non_null = col_data.dropna()
    if len(non_null) > 0:
        # 3. Nome puramente numérico + coluna numérica
        if name_str.lstrip("-").isdigit():
            numeric_ratio = sum(_is_numeric_value(v) for v in non_null) / len(non_null)
            if numeric_ratio > 0.5:
                return DATA_VALUE

        # 4. Token de texto nos dados da coluna
        if len(name_str) <= 20 and bool(_TOKEN_RE.match(name_str)):
            first_values = {str(v).strip() for v in non_null.head(20)}
            if name_str in first_values:
                return DATA_VALUE

    # 5. Alfanumérico descritivo
    if any(ch.isalpha() for ch in name_str):
        return REAL_NAME

    # Fallback: nome numérico em coluna não numérica — trata como dado
    return DATA_VALUE


def classificar_colunas(df: pd.DataFrame) -> list[str]:
    """Aplica ``classificar_coluna`` a cada coluna de dados (exclui o índice)."""
    return [classificar_coluna(str(name), pd.Series(df[name])) for name in df.columns]


class RodapeInfo(TypedDict):
    """Informação sobre o rodapé da tabela (últimas linhas)."""

    rows: list[RowInfo]
    n_non_data_rows: int
    has_metadata: bool
    last_data_row: int  # índice (0-based) da última linha de dados


def inspecionar_rodape(df: pd.DataFrame, n_lines: int = 4) -> RodapeInfo:
    """Inspeciona as últimas ``n_lines`` linhas em busca de metadados.

    Sinais de rodapé com metadados:
    * Linhas predominantemente vazias (sparse) no final.
    * Texto não numérico predominante nas últimas linhas (ex.: fonte, órgão,
      observações).
    * Transição abrupta de linhas densas (dados) para linhas esparsas (metadados).

    Retorna ``RodapeInfo`` com ``has_metadata=True`` e ``last_data_row`` quando
    metadados são detectados; ``has_metadata=False`` caso contrário.
    """
    n_rows = int(len(df))
    if n_rows == 0:
        return RodapeInfo(
            rows=[], n_non_data_rows=0, has_metadata=False, last_data_row=-1
        )

    n_cols = int(len(df.columns))
    sparse_threshold = max(2, 0.2 * n_cols)
    inspect_n = min(n_lines, n_rows)
    rows: list[RowInfo] = []
    n_metadata = 0
    last_data = n_rows - 1

    for offset in range(inspect_n):
        i = n_rows - 1 - offset
        row = df.iloc[i]
        non_null = row.dropna()
        n_non_empty = int(len(non_null))
        n_text = int(sum(_is_text_value(v) for v in non_null))
        text_ratio = (n_text / n_non_empty) if n_non_empty > 0 else 0.0
        # Uma linha de metadado é esparsa OU dominada por texto não numérico
        is_sparse = n_non_empty <= sparse_threshold
        is_text_heavy = n_non_empty >= 1 and text_ratio > 0.8
        is_metadata = is_sparse or is_text_heavy
        rows.append(
            RowInfo(
                index=i,
                n_non_empty=n_non_empty,
                n_text=n_text,
                is_header=False,
                is_sparse=is_sparse,
            )
        )
        if is_metadata:
            n_metadata += 1
            last_data = min(last_data, i - 1)
        else:
            # Linha densa com dados — para de inspecionar
            break

    # Metadados só se pelo menos 2 linhas consecutivas do final forem metadados
    # e a transição for abrupta (primeira linha não-metadado após metadados
    # indica fim dos dados).
    has_metadata = n_metadata >= 2
    if not has_metadata:
        last_data = n_rows - 1

    return RodapeInfo(
        rows=rows,
        n_non_data_rows=n_metadata,
        has_metadata=has_metadata,
        last_data_row=last_data,
    )


def inspecionar_linhas_cabecalho(
    df: pd.DataFrame, n_lines: int = 3
) -> HeaderInspection:
    """Identifica linhas candidatas a cabeçalho nas primeiras ``n_lines`` linhas.

    * ``is_header`` — a linha é candidata a cabeçalho quando tem pelo menos 2
      valores de texto não numérico E a razão de texto sobre valores não nulos
      é >= 0.7 (uma linha de nomes é dominada por texto; linhas de dados têm
      mistura de texto e números).
    * ``is_sparse`` — a linha tem poucos valores não nulos
      (``<= max(2, 20% das colunas)``), cobrindo linhas "vazias ou com poucos
      valores não nulos" (caso ``cabecalho_na_terceira_linha``).
    """
    n_cols = int(len(df.columns))
    sparse_threshold = max(2, 0.2 * n_cols)
    header_min_fill = max(2, 0.5 * n_cols)
    rows: list[RowInfo] = []
    n_empty_first = 0
    for i in range(min(n_lines, len(df))):
        row = df.iloc[i]
        non_null = row.dropna()
        n_non_empty = int(len(non_null))
        n_text = int(sum(_is_text_value(v) for v in non_null))
        text_ratio = (n_text / n_non_empty) if n_non_empty > 0 else 0.0
        # Uma linha de cabeçalho é dominada por texto E preenche a maioria das
        # colunas (não é uma linha esparsa com apenas um título/rótulo).
        is_header = n_text >= 2 and text_ratio >= 0.7 and n_non_empty >= header_min_fill
        is_sparse = n_non_empty <= sparse_threshold
        rows.append(
            RowInfo(
                index=i,
                n_non_empty=n_non_empty,
                n_text=n_text,
                is_header=is_header,
                is_sparse=is_sparse,
            )
        )
        if n_non_empty == 0:
            n_empty_first += 1
    return HeaderInspection(rows=rows, n_empty_first_rows=n_empty_first, n_cols=n_cols)
