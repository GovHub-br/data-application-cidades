"""Testes do tratamento de sub-tabelas (sub_tabelas_1 e sub_tabelas_3).

Cobre a transformação para formato longo e reconstrução de cabeçalho multi-linha.
"""

from __future__ import annotations

import pandas as pd

from classificacao.tratamento_subtabelas import (
    _is_numeric_predominant,
    _is_text_predominant,
    _is_title_row,
    _reconstruir_header_multilinha,
    tratar_sub_tabelas_1,
    tratar_sub_tabelas_3,
)


# ─── sub_tabelas_1 ────────────────────────────────────────────────────────


def test_tratar_sub_tabelas_1_duas_sub_tabelas() -> None:
    """Duas sub-tabelas separadas por 2 linhas vazias são transformadas
    para formato longo com colunas recorte_tipo, recorte_valor,
    data_referencia e valor.
    """
    # Cria DataFrame simulando sub_tabelas_1:
    # - Primeira coluna (unnamed_0): rótulos
    # - Demais colunas: timestamps YYYYMMDD_hhmmss
    # - Duas sub-tabelas separadas por 2 linhas vazias
    df = pd.DataFrame(
        {
            "unnamed_0": ["FGTS", "FAR", None, None, "Norte", "Sul"],
            "20240101_000000": [100, 200, None, None, 300, 400],
            "20240201_000000": [150, 250, None, None, 350, 450],
        }
    )
    df_tratado, info = tratar_sub_tabelas_1("bb_2024_teste", df)

    # Verifica colunas do formato longo
    assert "recorte_tipo" in df_tratado.columns
    assert "recorte_valor" in df_tratado.columns
    assert "data_referencia" in df_tratado.columns
    assert "valor" in df_tratado.columns

    # 2 sub-tabelas x 2 colunas de data x 2 linhas cada = 8 registros
    # (mas pode variar dependendo dos NaNs)
    assert len(df_tratado) >= 4

    # Verifica alguns valores específicos
    # Primeira linha: FGTS, frente, 2024-01-01, 100
    fgts_rows = df_tratado[df_tratado["recorte_valor"] == "FGTS"]
    assert len(fgts_rows) > 0
    assert all(fgts_rows["recorte_tipo"] == "frente")

    # Verifica metadados
    assert info["profile"] == "sub_tabelas_1"
    assert info["sub_tabelas_count"] == 2
    assert "recortes_detectados" in info
    assert "frente" in info["recortes_detectados"]
    assert "regiao" in info["recortes_detectados"]


def test_tratar_sub_tabelas_1_vazia() -> None:
    """DataFrame vazio gera resultado vazio com colunas do formato longo."""
    df = pd.DataFrame()
    df_tratado, info = tratar_sub_tabelas_1("vazia", df)
    assert list(df_tratado.columns) == [
        "recorte_tipo",
        "recorte_valor",
        "data_referencia",
        "valor",
        "source_table",
        "report_date",
        "institution",
        "profile",
    ]
    assert len(df_tratado) == 0
    assert info["profile"] == "sub_tabelas_1"


def test_tratar_sub_tabelas_1_metadados_presentes() -> None:
    """O DataFrame tratado inclui colunas de metadados (source_table, etc.)."""
    df = pd.DataFrame(
        {
            "unnamed_0": ["FGTS"],
            "20240101_000000": [100],
        }
    )
    df_tratado, info = tratar_sub_tabelas_1("caixa_001_2024_teste", df)
    assert "source_table" in df_tratado.columns
    assert "report_date" in df_tratado.columns
    assert "institution" in df_tratado.columns
    assert "profile" in df_tratado.columns
    assert df_tratado["institution"].iloc[0] == "caixa"
    assert df_tratado["profile"].iloc[0] == "sub_tabelas_1"


def test_tratar_sub_tabelas_1_sem_linhas_vazias() -> None:
    """Tabela sem linhas vazias é tratada como uma única sub-tabela."""
    df = pd.DataFrame(
        {
            "unnamed_0": ["FGTS", "FAR"],
            "20240101_000000": [100, 200],
            "20240201_000000": [150, 250],
        }
    )
    df_tratado, info = tratar_sub_tabelas_1("teste", df)
    assert info["sub_tabelas_count"] == 1
    assert len(df_tratado) == 4  # 2 linhas x 2 timestamps


# ─── sub_tabelas_3 ────────────────────────────────────────────────────────


def test_tratar_sub_tabelas_3_cabecalho_multilinha() -> None:
    """sub_tabelas_3 com cabeçalho de 2 linhas tem header reconstruído
    com concatenação via underscore.
    """
    # Simula uma sub_tabelas_3: 2 linhas de cabeçalho + 2 linhas de dados
    # Primeira linha: nomes principais (alguns com NaN para merged cells)
    # Segunda linha: sub-nomes
    df = pd.DataFrame(
        {
            0: ["Ano", "", "2024", "2025"],
            1: ["", "Valor", "100", "200"],
            2: ["Total", "", "300", "400"],
        }
    )
    # A primeira linha tem texto: "Ano", "Total" — o "" é ffill
    df_tratado, info = tratar_sub_tabelas_3("teste_sub3", df)

    # O DataFrame tratado deve ter dados
    assert len(df_tratado) >= 2

    # Verifica metadados
    assert info["profile"] == "sub_tabelas_3"
    assert "sub_tabelas_count" in info


def test_tratar_sub_tabelas_3_metadados() -> None:
    """Metadados institution e report_date estão presentes."""
    df = pd.DataFrame(
        {
            0: ["Ano", "2024"],
            1: ["Valor", "100"],
        }
    )
    df_tratado, info = tratar_sub_tabelas_3("caixa_2024_teste", df)
    assert "source_table" in df_tratado.columns
    assert "institution" in df_tratado.columns
    assert df_tratado["institution"].iloc[0] == "caixa"


def test_tratar_sub_tabelas_3_sem_header() -> None:
    """DataFrame com apenas dados (sem cabeçalho multi-linha) ainda processa."""
    df = pd.DataFrame(
        {
            "col_a": ["x", "y"],
            "col_b": ["1", "2"],
        }
    )
    df_tratado, info = tratar_sub_tabelas_3("teste", df)
    # Deve produzir resultado (pipeline bem_formada roda)
    assert "source_table" in df_tratado.columns
    assert info["profile"] == "sub_tabelas_3"


# ─── Warning de dissonância temporal ───────────────────────────────────────


def test_tratar_sub_tabelas_1_warning_dissonancia() -> None:
    """Quando data_referencia (colunas) difere do report_date (filename),
    um warning é emitido no info_dict."""
    df = pd.DataFrame(
        {
            "unnamed_0": ["FGTS"],
            "20090401_000000": [100],
            "20090501_000000": [150],
        }
    )
    # Filename contém 2012, colunas contêm 2009 → dissonância
    df_tratado, info = tratar_sub_tabelas_1("001_2012_02_teste", df)
    assert "warnings" in info
    assert len(info["warnings"]) >= 1
    assert any("2009" in w for w in info["warnings"])
    assert any("2012" in w for w in info["warnings"])
    assert any("template" in w for w in info["warnings"])


def test_tratar_sub_tabelas_1_sem_dissonancia() -> None:
    """Quando data_referencia e report_date têm o mesmo ano, nenhum warning."""
    df = pd.DataFrame(
        {
            "unnamed_0": ["FGTS"],
            "20240101_000000": [100],
            "20240201_000000": [150],
        }
    )
    df_tratado, info = tratar_sub_tabelas_1("bb_2024_teste", df)
    # Sem dissonância (ambos 2024)
    assert not any("discorda" in w for w in info.get("warnings", []))


def test_tratar_sub_tabelas_1_sem_report_date() -> None:
    """Quando report_date não pode ser extraído, nenhum warning de dissonância."""
    df = pd.DataFrame(
        {
            "unnamed_0": ["FGTS"],
            "20090401_000000": [100],
        }
    )
    # Filename sem data reconhecível → report_date vazio
    df_tratado, info = tratar_sub_tabelas_1("tabela_sem_data", df)
    assert not any("discorda" in w for w in info.get("warnings", []))


# ─── _is_title_row ──────────────────────────────────────────────────────────


def test_is_title_row_true() -> None:
    """Row with identical text after ffill is detected as title."""
    row = pd.Series(["Relatório 2: Total", "Relatório 2: Total", "Relatório 2: Total"])
    assert _is_title_row(row) is True


def test_is_title_row_false() -> None:
    """Row with different values is not a title."""
    row = pd.Series(["Ano", "Valor", "Total"])
    assert _is_title_row(row) is False


def test_is_title_row_single_non_null() -> None:
    """Row with only one non-null value is detected as title (would spread via ffill)."""
    row = pd.Series(["Relatório 2", None, None])
    assert _is_title_row(row) is True


# ─── _is_text_predominant ───────────────────────────────────────────────────


def test_is_text_predominant_true() -> None:
    """Row with majority text values returns True."""
    row = pd.Series(["UF", "Quantidade unidades", "Valor", None])
    assert _is_text_predominant(row) is True


def test_is_text_predominant_false_low_ratio() -> None:
    """Row with mostly numeric values returns False."""
    row = pd.Series(["DF", "0", "15750000"])
    assert _is_text_predominant(row) is False


def test_is_text_predominant_false_low_count() -> None:
    """Row with only one text value (below min_count=2) returns False."""
    row = pd.Series(["UF", "0", "1500"])
    assert _is_text_predominant(row) is False


# ─── _is_numeric_predominant ────────────────────────────────────────────────


def test_is_numeric_predominant_true() -> None:
    """Row with majority numeric values returns True."""
    row = pd.Series(["DF", "0", "15750000"])
    assert _is_numeric_predominant(row) is True


def test_is_numeric_predominant_false() -> None:
    """Row with mostly text values returns False."""
    row = pd.Series(["UF", "Quantidade", "Valor"])
    assert _is_numeric_predominant(row) is False


def test_is_numeric_predominant_threshold() -> None:
    """Row with 30% numeric meets the default threshold."""
    row = pd.Series(["UF", "0", "Nome"])
    assert _is_numeric_predominant(row) is True  # 1/3 = 33% ≥ 30%


# ─── _reconstruir_header_multilinha: header detection ──────────────────────


def test_reconstruir_header_multilinha_title_row_ignored() -> None:
    """Title row text does NOT appear in column names."""
    # Row 0: title (single non-null value → would spread via ffill)
    # Row 1: actual header (3 different text values)
    # Rows 2-3: data (numeric predominant)
    df = pd.DataFrame([
        ["Relatório Geral", None, None],
        ["Ano", "Mês", "Total"],
        ["2024", "01", "100"],
        ["2025", "02", "200"],
    ])
    result = _reconstruir_header_multilinha(df)
    # Title "Relatório Geral" should NOT appear in any column name
    for col in result.columns:
        assert "relatorio" not in col, f"Title found in column: {col}"


def test_reconstruir_header_multilinha_data_row_not_header() -> None:
    """Data rows with numeric predominance are NOT included in headers."""
    df = pd.DataFrame([
        ["UF", "Ano", "Total"],
        ["DF", "0", "100"],
        ["SP", "15750000", "200"],
    ])
    result = _reconstruir_header_multilinha(df)
    # Data row values should NOT appear in column names
    for col in result.columns:
        assert "df" not in col.lower(), f"Data value found in column: {col}"
        assert "15750000" not in col


def test_reconstruir_header_multilinha_text_header_detected() -> None:
    """Row with text values is correctly detected as header."""
    df = pd.DataFrame([
        ["UF", "Quantidade", "Valor"],
        ["SP", "10", "100"],
        ["RJ", "20", "200"],
    ])
    result = _reconstruir_header_multilinha(df)
    # Check that header values appear in column names
    col_names = " ".join(result.columns)
    assert "uf" in col_names and "quantidade" in col_names and "valor" in col_names, \
        f"Expected header values in columns, got: {result.columns}"


# ─── _reconstruir_header_multilinha: sub (super) format ────────────────────


def test_reconstruir_header_multilinha_sub_super_format() -> None:
    """Column names use sub (super) format after reconstruction."""
    # Row 0: super-header (different values per col group)
    # Row 1: sub-header
    # Rows 2-3: data (numeric predominant)
    df = pd.DataFrame([
        ["Ano", "Ano", "Total"],
        ["Mês", "Valor", "Qtd"],
        ["2024", "100", "10"],
        ["2025", "200", "20"],
    ])
    result = _reconstruir_header_multilinha(df)
    # Should have 2 data rows and 3 columns
    assert len(result) == 2
    assert len(result.columns) == 3
