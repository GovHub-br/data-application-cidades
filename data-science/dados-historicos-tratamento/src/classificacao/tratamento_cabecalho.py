"""Tratamento de tabelas com cabeçalho deslocado.

Cobre 6 subtipos mais cabeçalho composto:

- ``cabecalho_na_primeira_linha_1`` / ``_2``
- ``cabecalho_na_segunda_linha``
- ``cabecalho_na_terceira_linha_1`` / ``_2``
- ``cabecalho_composto_1`` / ``_2``
"""

from __future__ import annotations


import pandas as pd

from .profiling import _is_numeric_value, inspecionar_rodape
from .tratamento import (
    clean_dataframe,
    normalizar_nome_coluna,
    tratar_bem_formada,
)
from .tratamento_subtabelas import _is_title_row


def promover_header(df: pd.DataFrame, subtipo: str) -> pd.DataFrame:
    """Promove linha(s) de cabeçalho baseado no subtipo.

    * ``cabecalho_na_primeira_linha_1`` / ``_2``: linha 0 → header,
      descarta linha 0.
    * ``cabecalho_na_segunda_linha``: descarta linha 0, linha 1 → header.
    * ``cabecalho_na_terceira_linha_1`` / ``_2``: descarta linhas 0 e 1,
      linha 2 → header.

    Args:
        df: DataFrame bruto.
        subtipo: Subtipo de cabeçalho deslocado.

    Returns:
        DataFrame com cabeçalho promovido.
    """
    df = df.copy()

    if subtipo in (
        "cabecalho_na_primeira_linha_1",
        "cabecalho_na_primeira_linha_2",
        "cabecalho_na_primeira_linha",
    ):
        # Linha 0 é o cabeçalho real
        if len(df) >= 1:
            df.columns = [str(c) for c in df.iloc[0]]
            df = df.iloc[1:].reset_index(drop=True)

    elif subtipo == "cabecalho_na_segunda_linha":
        # Linha 0 é metadado ("Posicao: DD/MM/YYYY"), linha 1 é cabeçalho
        if len(df) >= 2:
            df.columns = [str(c) for c in df.iloc[1]]
            df = df.iloc[2:].reset_index(drop=True)

    elif subtipo in (
        "cabecalho_na_terceira_linha_1",
        "cabecalho_na_terceira_linha_2",
        "cabecalho_na_terceira_linha",
    ):
        # Linhas 0 e 1 são vazias/esparsas, linha 2 é cabeçalho
        if len(df) >= 3:
            df.columns = [str(c) for c in df.iloc[2]]
            df = df.iloc[3:].reset_index(drop=True)

    return df


def concatenar_header_composto(df: pd.DataFrame, n_linhas: int) -> pd.DataFrame:
    """Concatena cabeçalho composto de múltiplas linhas.

    Aplica forward-fill de NaN nas primeiras ``n_linhas`` (células
    mescladas), detecta e remove linhas de título (mesmo valor
    espalhado por todas as colunas), e concatena no formato
    ``sub (super)`` para formar o nome final da coluna.

    Args:
        df: DataFrame com cabeçalho composto nas primeiras linhas.
        n_linhas: Número de linhas de cabeçalho (2 ou 3).

    Returns:
        DataFrame com cabeçalho único.
    """
    if len(df) < n_linhas:
        return df

    df = df.copy()
    header_rows = df.iloc[:n_linhas].copy()
    data_rows = df.iloc[n_linhas:].copy()

    # Forward-fill horizontal (células mescladas)
    header_rows = header_rows.ffill(axis=1)

    # Detecta linhas de título entre os cabeçalhos (mesmo valor em todas as colunas)
    non_title_indices = [
        i for i in range(n_linhas) if not _is_title_row(header_rows.iloc[i])
    ]
    if not non_title_indices:
        non_title_indices = list(range(n_linhas))  # fallback: usa todas

    # Concatena com formato sub (super): última linha = sub, acima = super via ffill
    new_columns: list[str] = []
    for col_idx in range(len(header_rows.columns)):
        sub_val = ""
        super_parts: list[str] = []

        for row_idx in non_title_indices:
            val = header_rows.iloc[row_idx, col_idx]
            text = str(val).strip() if pd.notna(val) and str(val).strip() else ""
            if row_idx == non_title_indices[-1]:
                sub_val = text
            else:
                super_parts.append(text)

        # Merge super parts (ffill — repeat last non-empty)
        merged_super = ""
        for part in super_parts:
            if part:
                merged_super = part

        # Build name in sub (super) format
        if merged_super and sub_val and merged_super != sub_val:
            name = f"{sub_val} ({merged_super})"
        elif sub_val:
            name = sub_val
        elif merged_super:
            name = merged_super
        else:
            name = f"col_{col_idx}"

        new_columns.append(name)

    # Normaliza nomes
    new_columns = [normalizar_nome_coluna(n) for n in new_columns]

    data_rows.columns = new_columns
    return data_rows.reset_index(drop=True)


def remover_totalizacao(df: pd.DataFrame, subtipo: str) -> pd.DataFrame:
    """Remove linhas de totalização no final da tabela.

    Para ``cabecalho_na_primeira_linha_2``: verifica se a última linha
    tem predomínio numérico (>50% dos valores são numéricos) e a remove,
    juntamente com quaisquer linhas vazias antes dela.

    Args:
        df: DataFrame com cabeçalho já promovido.
        subtipo: Subtipo de cabeçalho.

    Returns:
        DataFrame sem linhas de totalização.
    """
    if subtipo not in ("cabecalho_na_primeira_linha_2",):
        return df

    if len(df) == 0:
        return df

    df = df.copy()

    # Remove linhas totalmente vazias do final
    while len(df) > 0 and df.iloc[-1].isna().all():
        df = df.iloc[:-1]

    # Verifica se a última linha tem predomínio numérico
    if len(df) > 0:
        last_row = df.iloc[-1].dropna()
        # Tenta converter valores para numérico
        numeric_count = 0
        for val in last_row:
            if _is_numeric_value(val):
                numeric_count += 1
        if len(last_row) > 0 and numeric_count / len(last_row) > 0.5:
            df = df.iloc[:-1]

    return df.reset_index(drop=True)


def remover_rodape(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas de metadados/rodapé do final da tabela.

    Usa ``inspecionar_rodape`` do módulo ``profiling`` para detectar
    linhas de metadados. Se detectadas, remove até a última linha de
    dados apontada pela inspeção.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame sem rodapé.
    """
    if len(df) == 0:
        return df

    rodape = inspecionar_rodape(df, n_lines=6)
    if rodape["has_metadata"]:
        last_data = rodape["last_data_row"]
        if 0 <= last_data < len(df):
            return df.iloc[: last_data + 1].reset_index(drop=True)

    return df.copy()


def tratar_cabecalho_deslocado(
    table_name: str,
    df: pd.DataFrame,
    subtipo: str,
) -> tuple[pd.DataFrame, dict]:
    """Pipeline completo para cabeçalho deslocado.

    Etapas:
    1. Promover header (conforme subtipo)
    2. Remover totalização (se ``primeira_linha_2``)
    3. Remover rodapé (se ``composto_1`` tem metadados)
    4. Concatenar header composto (se ``composto_1`` ou ``_2``)
    5. Aplicar pipeline ``tratar_bem_formada`` como ``colunar_denso``

    Args:
        table_name: Nome da tabela.
        df: DataFrame bruto.
        subtipo: Subtipo de cabeçalho deslocado.

    Returns:
        Tupla ``(df_tratado, info_dict)``.
    """
    df = df.copy()

    # Step 1: Promover header
    if subtipo in (
        "cabecalho_composto_1",
        "cabecalho_composto_2",
    ):
        # Header composto: não promove, vai concatenar depois
        pass
    else:
        df = promover_header(df, subtipo)

    # Step 2: Remover totalização
    df = remover_totalizacao(df, subtipo)

    # Step 3: Remover rodapé
    if subtipo == "cabecalho_composto_1":
        df = remover_rodape(df)

    # Step 4: Concatenar header composto
    if subtipo == "cabecalho_composto_1":
        df = concatenar_header_composto(df, n_linhas=3)
    elif subtipo == "cabecalho_composto_2":
        df = concatenar_header_composto(df, n_linhas=2)

    # Step 4.5: Limpeza pré-tratamento (remove linhas/colunas vazias)
    df = clean_dataframe(df)

    # Step 5: Aplicar pipeline bem_formada
    df_tratado, info = tratar_bem_formada(table_name, df)

    return df_tratado, info
