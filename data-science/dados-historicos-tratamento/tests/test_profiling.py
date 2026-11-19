"""Testes do profiling estrutural e da heurística R3a."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from classificacao.carregamento import carregar_csv
from classificacao.profiling import (
    DATA_VALUE,
    REAL_NAME,
    UNNAMED,
    classificar_coluna,
    classificar_colunas,
    inspecionar_linhas_cabecalho,
    profile_estrutural,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> pd.DataFrame:
    return carregar_csv(FIXTURES / f"{name}.csv")


# --- R3a: classificar_coluna -------------------------------------------------


def test_coluna_numerica_com_dados_numericos_eh_data_value() -> None:
    """Nome puramente numérico + coluna numérica → DATA_VALUE (spec R3a sinal 1)."""
    col = pd.Series(["304299", "304458", "304531", "306006"], dtype=str)
    assert classificar_coluna("304169", col) == DATA_VALUE


def test_coluna_nome_padrao_data_eh_data_value() -> None:
    """Nome no padrão YYYYMMDD_HHMMSS → DATA_VALUE (spec R3a sinal 2)."""
    col = pd.Series(["12", "36", "46"], dtype=str)
    assert classificar_coluna("20090401_000000", col) == DATA_VALUE


def test_coluna_nome_data_yyyymmdd_eh_data_value() -> None:
    col = pd.Series(["1", "2"], dtype=str)
    assert classificar_coluna("20090401", col) == DATA_VALUE


def test_coluna_nome_descritivo_real_eh_real_name() -> None:
    """Nome alfanumérico descritivo não encontrado nos dados → REAL_NAME."""
    col = pd.Series(["304169", "304299", "304458"], dtype=str)
    assert classificar_coluna("cod_contrato", col) == REAL_NAME


def test_coluna_unnamed_eh_unnamed() -> None:
    col = pd.Series(["a", "b"], dtype=str)
    assert classificar_coluna("unnamed_3", col) == UNNAMED
    assert classificar_coluna("Unnamed: 0", col) == UNNAMED
    assert classificar_coluna("", col) == UNNAMED


def test_coluna_token_encontrado_nos_dados_eh_data_value() -> None:
    """Token curto sem underscore que aparece como valor → DATA_VALUE (sinal 3)."""
    col = pd.Series(["FGTS", "FAR", "Entidades", "FGTS"], dtype=str)
    assert classificar_coluna("FGTS", col) == DATA_VALUE


def test_coluna_nome_numerico_coluna_nao_numerica_eh_data_value() -> None:
    """Fallback: nome numérico em coluna não numérica → DATA_VALUE."""
    col = pd.Series(["SP", "SC", "ES"], dtype=str)
    assert classificar_coluna("2", col) == DATA_VALUE


# --- classificar_colunas -----------------------------------------------------


def test_classificar_colunas_bem_formada() -> None:
    df = _fixture("bem_formada")
    assert classificar_colunas(df) == [REAL_NAME] * len(df.columns)


def test_classificar_colunas_sem_cabecalho() -> None:
    df = _fixture("sem_cabecalho")
    assert classificar_colunas(df) == [DATA_VALUE] * len(df.columns)


def test_classificar_colunas_nao_colunares() -> None:
    df = _fixture("nao_colunares_tipo1")
    assert classificar_colunas(df) == [UNNAMED] * len(df.columns)


# --- profile_estrutural ------------------------------------------------------


def test_profile_estrutural_bem_formada() -> None:
    df = _fixture("bem_formada")
    prof = profile_estrutural(df, file_size=209)
    assert prof["n_rows"] == 5
    assert prof["n_cols"] == 5
    assert prof["n_data_cols"] == 5  # índice excluído
    assert prof["file_size"] == 209
    assert prof["empty_row_ratio"] == 0.0


def test_profile_estrutural_nao_colunares_empty_ratio() -> None:
    df = _fixture("nao_colunares_tipo1")
    prof = profile_estrutural(df, file_size=167)
    assert prof["empty_row_ratio"] > 0.15


def test_profile_estrutural_vazia() -> None:
    df = _fixture("vazia")
    prof = profile_estrutural(df, file_size=11)
    assert prof["n_rows"] == 0
    assert prof["n_cols"] == 1
    assert prof["empty_row_ratio"] == 0.0


# --- inspecionar_linhas_cabecalho -------------------------------------------


def test_inspecionar_cabecalho_na_primeira_linha() -> None:
    df = _fixture("cabecalho_na_primeira_linha_1")
    insp = inspecionar_linhas_cabecalho(df)
    assert insp["rows"][0]["is_header"] is True
    assert insp["rows"][1]["is_header"] is False


def test_inspecionar_cabecalho_na_terceira_linha() -> None:
    df = _fixture("cabecalho_na_terceira_linha_1")
    insp = inspecionar_linhas_cabecalho(df)
    assert insp["rows"][0]["is_sparse"] is True
    assert insp["rows"][1]["is_sparse"] is True
    assert insp["rows"][2]["is_header"] is True


def test_inspecionar_bem_formada_sem_cabecalho_deslocado() -> None:
    df = _fixture("bem_formada")
    insp = inspecionar_linhas_cabecalho(df)
    assert all(not r["is_header"] for r in insp["rows"])
