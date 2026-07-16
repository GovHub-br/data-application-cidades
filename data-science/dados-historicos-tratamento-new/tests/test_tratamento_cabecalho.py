"""Testes do tratamento de cabeçalho deslocado.

Cobre promover_header, concatenar_header_composto, remover_totalizacao
e o pipeline tratar_cabecalho_deslocado.
"""

from __future__ import annotations

import pandas as pd

from classificacao.tratamento_cabecalho import (
    concatenar_header_composto,
    promover_header,
    remover_totalizacao,
    tratar_cabecalho_deslocado,
)


# ─── promover_header ──────────────────────────────────────────────────────


def test_promover_header_primeira_linha() -> None:
    """Linha 0 vira cabeçalho, linha 1+ viram dados."""
    df = pd.DataFrame({0: ["nome", "joao"], 1: ["idade", "30"]})
    result = promover_header(df, "cabecalho_na_primeira_linha_1")
    assert list(result.columns) == ["nome", "idade"]
    assert len(result) == 1
    assert result.iloc[0]["nome"] == "joao"


def test_promover_header_segunda_linha() -> None:
    """Linha 0 é descartada, linha 1 vira cabeçalho."""
    df = pd.DataFrame({0: ["Posicao: 01/2024", "nome", "joao"], 1: ["", "idade", "30"]})
    result = promover_header(df, "cabecalho_na_segunda_linha")
    assert list(result.columns) == ["nome", "idade"]
    assert len(result) == 1
    assert result.iloc[0]["nome"] == "joao"


def test_promover_header_terceira_linha() -> None:
    """Linhas 0 e 1 descartadas, linha 2 vira cabeçalho."""
    df = pd.DataFrame({0: ["", "", "nome", "joao"], 1: ["", "", "idade", "30"]})
    result = promover_header(df, "cabecalho_na_terceira_linha_1")
    assert list(result.columns) == ["nome", "idade"]
    assert len(result) == 1
    assert result.iloc[0]["nome"] == "joao"


def test_promover_header_subtipos_alternativos() -> None:
    """Aliases como 'cabecalho_na_primeira_linha' também funcionam."""
    df = pd.DataFrame({0: ["a", "1"], 1: ["b", "2"]})
    result = promover_header(df, "cabecalho_na_primeira_linha")
    assert list(result.columns) == ["a", "b"]


def test_promover_header_df_vazio() -> None:
    """DataFrame vazio retorna inalterado."""
    df = pd.DataFrame()
    result = promover_header(df, "cabecalho_na_primeira_linha_1")
    assert result.empty


# ─── concatenar_header_composto ────────────────────────────────────────────


def test_concatenar_header_composto_2_linhas() -> None:
    """Duas linhas de cabeçalho são concatenadas com formato sub (super)."""
    df = pd.DataFrame(
        {
            0: ["Ano", "2024", "100"],
            1: ["", "Valor", "200"],
        }
    )
    # header_rows = linhas 0-1 (n_linhas=2)
    # Row 0 after ffill: ["Ano", "Ano"] → título (mesmo valor) → ignorada
    # Row 1: ["2024", "Valor"] → sub-header
    # Sem super-header → colunas: "2024"→"col", "Valor"→"valor"
    # data_rows = linha 2
    result = concatenar_header_composto(df, n_linhas=2)
    assert len(result) == 1
    assert len(result.columns) == 2


def test_concatenar_header_composto_com_ffill() -> None:
    """Forward-fill preenche células mescladas antes da concatenação."""
    df = pd.DataFrame(
        {
            0: ["Categoria", "Frutas", "Maçã"],
            1: ["", "Legumes", "Cenoura"],
            2: ["", "", "Batata"],
        }
    )
    # n_linhas=2: header = linhas 0-1
    # Row 0 after ffill: ["Categoria", "Categoria"] → título → ignorada
    # Row 1: ["Frutas", "Legumes"] → sub-header
    # Sem super-header → colunas: "frutas", "legumes"
    result = concatenar_header_composto(df, n_linhas=2)
    assert len(result) == 1  # apenas a linha de dados (maçã, cenoura)
    # Mantém 3 colunas originais (0, 1, 2)


# ─── remover_totalizacao ──────────────────────────────────────────────────


def test_remover_totalizacao_ultima_linha_numerica() -> None:
    """Última linha com predomínio numérico é removida."""
    df = pd.DataFrame({"a": ["x", "y", "100"], "b": ["1", "2", "200"]})
    result = remover_totalizacao(df, "cabecalho_na_primeira_linha_2")
    assert len(result) == 2
    assert result.iloc[-1]["a"] == "y"


def test_remover_totalizacao_nao_aplica_para_outro_subtipo() -> None:
    """Apenas subtipo 'cabecalho_na_primeira_linha_2' remove totalização."""
    df = pd.DataFrame({"a": ["x", "y", "100"], "b": ["1", "2", "200"]})
    result = remover_totalizacao(df, "bem_formada")
    assert len(result) == 3


def test_remover_totalizacao_vazio() -> None:
    """DataFrame vazio retorna vazio."""
    df = pd.DataFrame()
    result = remover_totalizacao(df, "cabecalho_na_primeira_linha_2")
    assert result.empty


# ─── tratar_cabecalho_deslocado (smoke) ───────────────────────────────────


def test_tratar_cabecalho_deslocado_primeira_linha() -> None:
    """Pipeline completo para cabecalho_composto_2 via
    tratar_cabecalho_deslocado produz saída com metadados.
    """
    # Simula cabecalho_composto_2: 2 linhas de header composto + dados
    df = pd.DataFrame(
        {
            0: ["Ano", "2024", "100", "200"],
            1: ["", "Valor", "150", "250"],
        }
    )
    df_t, info = tratar_cabecalho_deslocado("bb_2024_teste", df, "cabecalho_composto_2")
    # Deve ter produzido saída com metadados
    assert "source_table" in df_t.columns
    assert "institution" in df_t.columns
    assert "profile" in df_t.columns
    assert info["profile"] in ("colunar_denso", "lookup")


def test_tratar_cabecalho_deslocado_primeira_linha_1() -> None:
    """Pipeline para cabecalho_na_primeira_linha_1."""
    df = pd.DataFrame({0: ["nome", "joao", "maria"], 1: ["idade", "30", "25"]})
    df_t, info = tratar_cabecalho_deslocado(
        "bb_teste", df, "cabecalho_na_primeira_linha_1"
    )
    assert "source_table" in df_t.columns
    assert len(df_t) == 2
    assert info["profile"] in ("colunar_denso", "lookup")
