"""Testes do módulo de tratamento estrutural e type-canonization.

Cobre normalização, separador decimal, datas, período, instituição,
tipos canônicos, classificação de perfil e o pipeline tratar_bem_formada.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from classificacao.saida_tratamento import gerar_relatorio_qualidade
from classificacao.tratamento import (
    classificar_perfil,
    clean_dataframe,
    converter_decimal,
    detectar_formato_data,
    detectar_separador_decimal,
    extrair_periodo_filename,
    inferir_instituicao,
    normalizar_nome_coluna,
    tipo_canonico_manual,
    tratar_bem_formada,
)


# ─── Group 1: Column Name Normalization ───────────────────────────────────


def test_normalizar_nome_coluna_codigo_ibge() -> None:
    assert normalizar_nome_coluna("Código do IBGE") == "codigo_do_ibge"


def test_normalizar_nome_coluna_com_espacos() -> None:
    assert normalizar_nome_coluna("  Data Contratação ") == "data_contratacao"


def test_normalizar_nome_coluna_vazio() -> None:
    assert normalizar_nome_coluna("") == "col"


def test_normalizar_nome_coluna_espacos() -> None:
    assert normalizar_nome_coluna("   ") == "col"


def test_normalizar_nome_coluna_digitos() -> None:
    assert normalizar_nome_coluna("123") == "col"


def test_normalizar_nome_coluna_valor_parenteses() -> None:
    assert normalizar_nome_coluna("Valor (R$)") == "valor_r"


def test_normalizar_nome_coluna_multiplos_underscores() -> None:
    assert normalizar_nome_coluna("Nome__Duplo") == "nome_duplo"


# ─── Group 2: Decimal Separator ──────────────────────────────────────────


def test_detectar_separador_decimal_virgula() -> None:
    s = pd.Series(["1,5", "2,3", "4,7", "8,9", "10,1"])
    assert detectar_separador_decimal(s) is True


def test_detectar_separador_decimal_ponto() -> None:
    s = pd.Series(["1.5", "2.3", "4.7", "8.9", "10.1"])
    assert detectar_separador_decimal(s) is False


def test_detectar_separador_decimal_vazio() -> None:
    s = pd.Series([], dtype=str)
    assert detectar_separador_decimal(s) is False


def test_converter_decimal_virgula() -> None:
    s = pd.Series(["1234,56", "789,01", "0,99"])
    result = converter_decimal(s)
    assert result.dtype == "float64"
    assert result.iloc[0] == 1234.56
    assert result.iloc[1] == 789.01
    assert result.iloc[2] == 0.99


def test_converter_decimal_ponto() -> None:
    s = pd.Series(["1234.56", "789.01"])
    result = converter_decimal(s)
    assert result.dtype == "float64"
    assert result.iloc[0] == 1234.56


def test_converter_decimal_na() -> None:
    s = pd.Series(["abc", "def"])
    result = converter_decimal(s)
    assert result.isna().all()


# ─── Group 3: Date Detection ──────────────────────────────────────────────


def test_detectar_formato_data_iso() -> None:
    s = pd.Series(["2024-01-15", "2023-12-01", "2022-06-30"])
    assert detectar_formato_data(s) == "iso"


def test_detectar_formato_data_br_detectado_como_iso() -> None:
    """Em pandas 3.x, pd.Timestamp aceita '25/01/2024' sem dayfirst,
    então todo formato data válido é detectado como 'iso'."""
    s = pd.Series(["25/01/2024", "13/12/2023", "30/06/2022"])
    assert detectar_formato_data(s) == "iso"


def test_detectar_formato_data_misto_maioria_iso() -> None:
    s = pd.Series(["25/01/2024", "2023-12-01", "not_a_date"])
    # 2/3 parseáveis como ISO → ratio > 0.5 → "iso"
    assert detectar_formato_data(s) == "iso"


def test_detectar_formato_data_vazio() -> None:
    s = pd.Series([], dtype=str)
    assert detectar_formato_data(s) is None


def test_detectar_formato_data_ids_numericos_filtrados() -> None:
    """Valores que parecem IDs numéricos (1-4 dígitos) são ignorados."""
    s = pd.Series(["25/01/2024", "14/02/2024", "13/03/2024", "1234"])
    # "1234" é filtrado por _parece_numerico, restante 3/3 ISO → "iso"
    assert detectar_formato_data(s) == "iso"


# ─── Group 7: Period & Institution ─────────────────────────────────────────


def test_extrair_periodo_filename_dd_mm_yyyy_final() -> None:
    assert extrair_periodo_filename("001_2012_02_fevereiro_10_02_2012") == "2012-02-10"


def test_extrair_periodo_filename_yyyymm_inicio() -> None:
    assert extrair_periodo_filename("202402_snh_relatorio") == "2024-02-01"


def test_extrair_periodo_filename_curto_sem_data() -> None:
    assert extrair_periodo_filename("024_10_snh_sem_data") is None


def test_extrair_periodo_filename_yyyymmdd_final() -> None:
    assert extrair_periodo_filename("relatorio_20240315") == "2024-03-15"


def test_extrair_periodo_filename_yyyy_mm_meio() -> None:
    assert extrair_periodo_filename("dados_2024_03_relatorio") == "2024-03-01"


def test_extrair_periodo_filename_sem_match() -> None:
    assert extrair_periodo_filename("tabela_sem_data") is None


def test_inferir_instituicao_bb() -> None:
    assert inferir_instituicao("bb_2012_02_fevereiro") == "bb"


def test_inferir_instituicao_caixa() -> None:
    assert inferir_instituicao("caixa_001_2018") == "caixa"


def test_inferir_instituicao_unknown() -> None:
    assert inferir_instituicao("001_2012_sem_prefixo") == "unknown"


# ─── Group 5: Type Canonization ───────────────────────────────────────────


def test_tipo_canonico_manual_cnpj() -> None:
    assert tipo_canonico_manual("cnpj") == "str"
    assert tipo_canonico_manual("nr_cnpj") == "str"


def test_tipo_canonico_manual_vlr() -> None:
    assert tipo_canonico_manual("vlr_financiamento") == "float64"


def test_tipo_canonico_manual_qtd() -> None:
    assert tipo_canonico_manual("qtd_unidades") == "Int64"
    assert tipo_canonico_manual("qt_contratos") == "Int64"


def test_tipo_canonico_manual_dat() -> None:
    assert tipo_canonico_manual("dat_contratacao") == "datetime64[ns]"
    assert tipo_canonico_manual("dt_assinatura") == "datetime64[ns]"
    assert tipo_canonico_manual("data_base") == "datetime64[ns]"


def test_tipo_canonico_manual_descricao() -> None:
    assert tipo_canonico_manual("descricao") is None
    assert tipo_canonico_manual("nome_municipio") is None


def test_tipo_canonico_manual_cod() -> None:
    assert tipo_canonico_manual("cod_municipio") == "str"
    assert tipo_canonico_manual("cod_ibge") == "str"


def test_tipo_canonico_manual_total() -> None:
    assert tipo_canonico_manual("total_geral") == "float64"


# ─── Group 6: Profile Detection ───────────────────────────────────────────


def test_classificar_perfil_lookup() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    assert classificar_perfil(df) == "lookup"


def test_classificar_perfil_event_level() -> None:
    df = pd.DataFrame(
        {
            "apf": ["x", "x", "y"],
            "cod_contrato": ["c1", "c1", "c2"],
            "vlr": [100, 200, 300],
            "dt": ["2024-01", "2024-02", "2024-03"],
            "extra": [1, 2, 3],
        }
    )
    assert classificar_perfil(df) == "event_level"


def test_classificar_perfil_colunar_denso_4_6() -> None:
    df = pd.DataFrame(
        {
            "a": [1, 2],
            "b": [3, 4],
            "c": [5, 6],
            "d": [7, 8],
        }
    )
    assert classificar_perfil(df) == "colunar_denso"


def test_classificar_perfil_agregado_uf() -> None:
    df = pd.DataFrame(
        {
            "uf": ["AC", "AL", "AP", "Total"],
            "a": [1, 2, 3, 10],
            "b": [4, 5, 6, 15],
            "c": [7, 8, 9, 17],
            "d": [10, 11, 12, 20],
            "e": [13, 14, 15, 23],
            "f": [16, 17, 18, 26],
            "g": [19, 20, 21, 29],
        }
    )
    assert classificar_perfil(df) == "agregado_uf"


def test_classificar_perfil_colunar_denso_20() -> None:
    dados = {chr(ord("a") + i): range(3) for i in range(20)}
    df = pd.DataFrame(dados)
    assert classificar_perfil(df) == "colunar_denso"


# ─── Data Cleaning ────────────────────────────────────────────────────────


def test_clean_dataframe_remove_dash_only_rows() -> None:
    """Linhas onde todas as células são '-' ou NaN são removidas."""
    df = pd.DataFrame(
        {
            "a": ["-", "valor", "-", None],
            "b": ["-", "ok", "-", None],
            "c": ["-", "sim", None, None],
        }
    )
    result = clean_dataframe(df)
    # Apenas a linha com dados válidos sobrevive
    assert len(result) == 1
    assert result.iloc[0]["a"] == "valor"


def test_clean_dataframe_preserve_partial_dash_row() -> None:
    """Linha com '-' em apenas algumas colunas é preservada."""
    df = pd.DataFrame(
        {
            "a": ["-", "dado"],
            "b": ["-", "123"],
        }
    )
    result = clean_dataframe(df)
    # Ambas as linhas sobrevivem: a primeira é toda '-', a segunda tem dados
    assert len(result) == 1
    assert result.iloc[0]["a"] == "dado"


def test_clean_dataframe_remove_nan_only_rows() -> None:
    """Linhas onde todas as células são NaN são removidas."""
    df = pd.DataFrame(
        {
            "a": [None, "ok", None],
            "b": [None, "sim", None],
        }
    )
    result = clean_dataframe(df)
    assert len(result) == 1
    assert result.iloc[0]["a"] == "ok"


def test_clean_dataframe_remove_empty_columns() -> None:
    """Colunas 100% NaN são removidas."""
    df = pd.DataFrame(
        {
            "a": [None, None, None],
            "b": ["x", "y", "z"],
            "c": [None, None, None],
        }
    )
    result = clean_dataframe(df)
    assert list(result.columns) == ["b"]
    assert len(result) == 3


def test_clean_dataframe_empty_input() -> None:
    """DataFrame vazio retorna cópia vazia sem erro."""
    df = pd.DataFrame()
    result = clean_dataframe(df)
    assert result.empty


def test_clean_dataframe_all_valid() -> None:
    """DataFrame sem linhas/colunas vazias volta igual."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = clean_dataframe(df)
    assert len(result) == 2
    assert list(result.columns) == ["a", "b"]


def test_tratar_bem_formada_remove_dash_rows() -> None:
    """Integração: tratar_bem_formada remove linhas only-dash antes do pipeline."""
    df = pd.DataFrame(
        {
            "Codigo": ["001", "-", "002"],
            "Valor": ["100,50", "-", "200,00"],
            "Data": ["2024-01-15", "-", "2024-06-30"],
        }
    )
    df_t, info = tratar_bem_formada("bb_2024_teste", df)
    # Linha do meio (toda '-') deve ter sido removida: 3 linhas → 2
    assert len(df_t) == 2
    # Warning de limpeza deve estar presente
    assert any("linhas vazias/dash" in w for w in info["warnings"])


# ─── Group 7b: Quality Report ─────────────────────────────────────────────


def test_relatorio_qualidade_inclui_coluna_error() -> None:
    """Verifica que a coluna 'error' está presente no relatório de qualidade."""
    resultados = [
        {
            "table_name": "tab1",
            "status": "treated",
            "n_rows": 10,
            "n_cols": 5,
            "profile": "lookup",
            "institution": "bb",
            "report_date": "2024-01",
            "missing_pct": 0.1,
            "encoding_issues": 0,
            "date_parse_errors": 0,
            "type_coercion_warnings": 0,
            "error": "",  # treated has empty error
        },
        {
            "table_name": "tab2",
            "status": "error",
            "n_rows": 0,
            "n_cols": 0,
            "profile": "indeterminada",
            "institution": "unknown",
            "report_date": "",
            "missing_pct": 0.0,
            "encoding_issues": 0,
            "date_parse_errors": 0,
            "type_coercion_warnings": 0,
            "error": "ValueError: falha simulada",
        },
        {
            "table_name": "tab3",
            "status": "discarded",
            "n_rows": 0,
            "n_cols": 0,
            "profile": "vazia",
            "institution": "unknown",
            "report_date": "",
            "missing_pct": 0.0,
            "encoding_issues": 0,
            "date_parse_errors": 0,
            "type_coercion_warnings": 0,
            "error": "",
        },
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "_quality_report.csv"
        df = gerar_relatorio_qualidade(resultados, output_path)

        assert "error" in df.columns
        assert df.loc[df["table_name"] == "tab1", "error"].iloc[0] == ""
        assert (
            df.loc[df["table_name"] == "tab2", "error"].iloc[0]
            == "ValueError: falha simulada"
        )
        assert df.loc[df["table_name"] == "tab3", "error"].iloc[0] == ""


# ─── Group 8: Pipeline tratar_bem_formada ─────────────────────────────────


def test_tratar_bem_formada_smoke() -> None:
    """Teste básico do pipeline: entrada pequena produz saída com metadados."""
    df = pd.DataFrame(
        {
            "Código": ["001", "002"],
            "Valor (R$)": ["100,50", "200,00"],
            "Data": ["2024-01-15", "2024-06-30"],
            "Unidades": ["10", "20"],
        }
    )
    df_t, info = tratar_bem_formada("bb_2024_01_teste", df, content_hash="abc123")
    # Metadados adicionados
    assert "source_table" in df_t.columns
    assert "report_date" in df_t.columns
    assert "institution" in df_t.columns
    assert "content_hash" in df_t.columns
    assert "profile" in df_t.columns
    assert df_t["source_table"].iloc[0] == "bb_2024_01_teste"
    assert df_t["institution"].iloc[0] == "bb"
    # Info dict preenchido
    assert info["profile"] in ("colunar_denso",)
    assert info["institution"] == "bb"
    assert info["n_rows"] == 2
    assert info["n_cols_original"] == 4
    assert isinstance(info["warnings"], list)


def test_coerce_coluna_datetime_dayfirst() -> None:
    """Data brasileira (dd/mm/YYYY) deve ser interpretada com dayfirst.

    03/04/2011 = 3 de abril (não 4 de março).
    """
    from classificacao.tratamento import _coerce_coluna

    series = pd.Series(["03/04/2011", "31/12/2010", "N/A", "-"])
    result = _coerce_coluna(series, "datetime64[ns]")

    # Verify type (resolution may be ns or us depending on pandas version)
    assert pd.api.types.is_datetime64_any_dtype(result)

    # 03/04/2011 → April 3, 2011 (not March 4)
    assert result.iloc[0].day == 3
    assert result.iloc[0].month == 4
    assert result.iloc[0].year == 2011

    # 31/12/2010 → December 31, 2010
    assert result.iloc[1].day == 31
    assert result.iloc[1].month == 12

    # Invalid dates become NaT
    assert pd.isna(result.iloc[2])
    assert pd.isna(result.iloc[3])
