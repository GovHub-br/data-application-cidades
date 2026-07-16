"""Testes do modulo de inferencia de nomes de colunas."""

from __future__ import annotations

import pandas as pd

from classificacao.inferencia_colunas import (
    _desambiguar_codigos,
    _desambiguar_datas,
    _desambiguar_uh,
    _desambiguar_valores,
    _garantir_unicidade,
    _mediana_numerica,
    _normalizar_nome,
    _ratio_match,
    detectar_cnpj,
    detectar_codigo,
    detectar_data,
    detectar_data_referencia,
    detectar_endereco,
    detectar_faixa_mcmv,
    detectar_frente,
    detectar_nome_empresa,
    detectar_nome_empreendimento,
    detectar_percentual,
    detectar_situacao_obra,
    detectar_uh,
    detectar_valor_monetario,
    inferir_nomes_colunas,
)
from classificacao.carregamento import carregar_amostra


# ─── Helpers ──────────────────────────────────────────────────────────────


def _serie(valores: list) -> pd.Series:
    return pd.Series(valores, dtype=str)


# ─── Utility functions ────────────────────────────────────────────────────


def test_ratio_match_all_match() -> None:
    s = _serie(["far", "FAR", "far", "far", None])
    assert _ratio_match(s, lambda v: str(v).strip().lower() == "far") == 1.0


def test_ratio_match_half_match() -> None:
    s = _serie(["far", "fgts", "far", "fgts"])
    assert _ratio_match(s, lambda v: str(v).strip().lower() == "far") == 0.5


def test_ratio_match_empty() -> None:
    s = _serie([None, None])
    assert _ratio_match(s, lambda v: True) == 0.0


def test_mediana_numerica_comma() -> None:
    s = _serie(["100,00", "200,00", "300,00"])
    assert _mediana_numerica(s) == 200.0


def test_mediana_numerica_mixed() -> None:
    """Mediana de [100, 200.50] = 150.25."""
    s = _serie(["100", "200,50", "abc", None])
    assert _mediana_numerica(s) == 150.25


def test_normalizar_nome() -> None:
    assert _normalizar_nome("Código do IBGE") == "codigo_do_ibge"


def test_normalizar_nome_acentos() -> None:
    assert _normalizar_nome("CONCLUÍDA") == "concluida"


def test_garantir_unicidade() -> None:
    assert _garantir_unicidade(["a", "b", "a", "c"]) == ["a", "b", "a_1", "c"]


def test_garantir_unicidade_no_dups() -> None:
    assert _garantir_unicidade(["a", "b", "c"]) == ["a", "b", "c"]


# ─── detectar_situacao_obra ───────────────────────────────────────────────


def test_situacao_obra_positive() -> None:
    s = _serie(["CONCLUÍDA", "ATRASADA", "NORMAL", "PARALISADA", "CONCLUÍDA"])
    assert detectar_situacao_obra(s) == "situacao_obra"


def test_situacao_obra_sem_medicao() -> None:
    s = _serie(["SEM MEDIÇÃO", "SEM MEDIÇÃO", "SEM MEDIÇÃO"])
    assert detectar_situacao_obra(s) == "situacao_obra"


def test_situacao_obra_negative() -> None:
    s = _serie(["FAR", "FGTS", "RURAL"])
    assert detectar_situacao_obra(s) is None


# ─── detectar_frente ──────────────────────────────────────────────────────


def test_frente_positive() -> None:
    s = _serie(["FAR", "FAR", "FGTS", "FAR", "RURAL"])
    assert detectar_frente(s) == "frente"


def test_frente_negative() -> None:
    s = _serie(["CONCLUÍDA", "ATRASADA"])
    assert detectar_frente(s) is None


# ─── detectar_data_referencia ─────────────────────────────────────────────


def test_data_referencia_positive() -> None:
    s = _serie(["2019-05-30"] * 20)
    assert detectar_data_referencia(s) == "data_referencia"


def test_data_referencia_two_values() -> None:
    s = _serie(["2019-05-30"] * 10 + ["2019-05-31"] * 10)
    assert detectar_data_referencia(s) == "data_referencia"


def test_data_referencia_many_values() -> None:
    s = _serie([f"2019-05-{i:02d}" for i in range(1, 20)])
    assert detectar_data_referencia(s) is None


# ─── detectar_cnpj ────────────────────────────────────────────────────────


def test_cnpj_digits_14() -> None:
    s = _serie(["11010326000116"] * 10)
    assert detectar_cnpj(s) == "cnpj"


def test_cnpj_digits_13() -> None:
    s = _serie(["4588068000194"] * 10)
    assert detectar_cnpj(s) == "cnpj"


def test_cnpj_formatted() -> None:
    s = _serie(["11.010.326/0001-16"] * 10)
    assert detectar_cnpj(s) == "cnpj"


def test_cnpj_mixed() -> None:
    s = _serie(["11010326000116", "45.880.680/0001-94", "4588068000194"] * 5)
    assert detectar_cnpj(s) == "cnpj"


# ─── detectar_faixa_mcmv ──────────────────────────────────────────────────


def test_faixa_positive() -> None:
    s = _serie(["1", "2", "1", "3", "1", "1", "1"])
    assert detectar_faixa_mcmv(s) == "faixa_mcmv"


def test_faixa_negative_high_value() -> None:
    s = _serie(["5", "6", "7"])
    assert detectar_faixa_mcmv(s) is None


# ─── detectar_percentual ──────────────────────────────────────────────────


def test_percentual_positive() -> None:
    s = _serie(["72,33", "85,69", "100", "93,31", "98,73", "99,94", "0"])
    assert detectar_percentual(s) == "valor_percentual_conclusao"


def test_percentual_integer() -> None:
    s = _serie(["100", "0", "50", "75", "25"])
    assert detectar_percentual(s) == "valor_percentual_conclusao"


def test_percentual_all_zeros() -> None:
    """Coluna com apenas zeros nao deve ser percentual."""
    s = _serie(["0", "0,00", "0", "0,00"] * 5)
    assert detectar_percentual(s) is None


def test_percentual_negative() -> None:
    s = _serie(["ABC", "DEF"])
    assert detectar_percentual(s) is None


# ─── detectar_data ────────────────────────────────────────────────────────


def test_data_iso() -> None:
    s = _serie(["2013-01-31", "2013-12-27", "2014-06-30"] * 5)
    assert detectar_data(s) == "data"


def test_data_yyyymmdd() -> None:
    s = _serie(["20130731", "20131227", "20140630"] * 5)
    assert detectar_data(s) == "data"


# ─── detectar_nome_empreendimento ─────────────────────────────────────────


def test_empreendimento_positive() -> None:
    s = _serie(
        [
            "RESIDENCIAL CAMINHO DO SOL",
            "CONJ HAB ULYSSES GUIMARAES",
            "PARQUE ARCO IRIS",
            "JARDIM CANGURU",
            "VIVER MELHOR MARITUBA",
        ]
        * 3
    )
    assert detectar_nome_empreendimento(s) == "nome_empreendimento"


def test_empreendimento_negative() -> None:
    s = _serie(["123", "456", "789"] * 5)
    assert detectar_nome_empreendimento(s) is None


# ─── detectar_endereco ────────────────────────────────────────────────────


def test_endereco_positive() -> None:
    s = _serie(
        [
            "RUA DA FAIXA, S/N",
            "ESTRADA VICINAL S/N",
            "AVENIDA BABI",
            "LOTE DE TERRAS SOB NUMERO 171",
        ]
        * 3
    )
    assert detectar_endereco(s) == "endereco"


def test_endereco_negative() -> None:
    s = _serie(["FAR", "FGTS"])
    assert detectar_endereco(s) is None


# ─── detectar_nome_empresa ────────────────────────────────────────────────


def test_empresa_positive() -> None:
    s = _serie(
        [
            "L. T. O. INCORPORACOES E CONSTRUCOES LTDA",
            "LBX CONSTRUCAO CIVIL LTDA",
            "DIRECIONAL ENGENHARIA S.A.",
        ]
        * 5
    )
    assert detectar_nome_empresa(s) == "nome_empresa"


def test_empresa_em_recuperacao() -> None:
    s = _serie(["AURORA CONSTRUCOES INCORPO SV LTDA - EM RECUPERACAO"] * 15)
    assert detectar_nome_empresa(s) == "nome_empresa"


# ─── detectar_valor_monetario ─────────────────────────────────────────────


def test_valor_monetario_comma() -> None:
    s = _serie(["35640000,00", "34200000,00", "2834387,76"] * 5)
    assert detectar_valor_monetario(s) == "valor_monetario"


def test_valor_monetario_large_int() -> None:
    s = _serie(["1498000000", "15000000", "20000000"] * 5)
    assert detectar_valor_monetario(s) == "valor_monetario"


def test_valor_monetario_comma_with_dots() -> None:
    s = _serie(["1.498.000,00", "35.640.000,00"] * 5)
    assert detectar_valor_monetario(s) == "valor_monetario"


# ─── detectar_uh ──────────────────────────────────────────────────────────


def test_uh_positive() -> None:
    s = _serie(["214", "660", "600", "47"] * 5)
    assert detectar_uh(s) == "qtd_uh"


def test_uh_with_zeros() -> None:
    s = _serie(["0", "0", "46", "285", "0"] * 4)
    assert detectar_uh(s) == "qtd_uh"


# ─── detectar_codigo ──────────────────────────────────────────────────────


def test_codigo_6_digits() -> None:
    s = _serie(["354260", "210675", "211270"] * 10)
    assert detectar_codigo(s) == "codigo"


def test_codigo_5_digits() -> None:
    s = _serie(["12345", "67890"] * 10)
    assert detectar_codigo(s) == "codigo"


def test_codigo_7_digits() -> None:
    s = _serie(["1050308", "1119039"] * 10)
    assert detectar_codigo(s) == "codigo"


# ─── Threshold tests ──────────────────────────────────────────────────────


def test_threshold_below_70() -> None:
    """Coluna com exatamente 69% de match nao deve ativar detector de 70%."""
    # 69 valores match, 31 nao
    vals = ["FAR"] * 69 + ["INVALIDO"] * 31
    s = _serie(vals)
    assert detectar_frente(s) is None


def test_threshold_exactly_70() -> None:
    """Coluna com exatamente 70% deve ativar."""
    vals = ["FAR"] * 70 + ["INVALIDO"] * 30
    s = _serie(vals)
    assert detectar_frente(s) == "frente"


# ─── Priority test ────────────────────────────────────────────────────────


def test_priority_percentual_before_faixa() -> None:
    """Valores 1-4 com virgula decimal devem ser percentual, nao faixa."""
    s = _serie(["1,50", "2,30", "3,75", "4,00"] * 5)
    result = inferir_nomes_colunas(pd.DataFrame({"a": s}))
    # faixa runs first, but "1,50" doesn't match {1,2,3,4} because it's "1,50" not "1"
    # So it should fall through to percentual
    assert "valor_percentual_conclusao" in result[0][0]


def test_priority_faixa_before_percentual() -> None:
    """Inteiros 1-4 devem ser faixa, nao percentual."""
    s = _serie(["1", "2", "3", "1", "2"] * 5)
    result = inferir_nomes_colunas(pd.DataFrame({"a": s}))
    assert result[0][0] == "faixa_mcmv"


# ─── Desambiguacao: datas ────────────────────────────────────────────────


def test_desambiguar_datas() -> None:
    df = pd.DataFrame(
        {
            "a": ["2013-01-31", "2013-12-27", "2014-06-30"] * 5,
            "b": ["2016-01-31", "2018-08-01", "2014-08-31"] * 5,
            "c": ["2019-05-30"] * 15,
        }
    )
    nomes = ["data", "data", "data"]
    result = _desambiguar_datas(nomes, df)
    assert result[0] == "data_inicio"
    assert result[1] == "data_fim"
    assert result[2] == "data_referencia"


# ─── Desambiguacao: valores ───────────────────────────────────────────────


def test_desambiguar_valores() -> None:
    df = pd.DataFrame(
        {
            "a": ["100,00"] * 10,  # mediana 100
            "b": ["1000000,00"] * 10,  # mediana 1M -> maior
            "c": ["500,00"] * 10,  # mediana 500
        }
    )
    nomes = ["valor_monetario", "valor_monetario", "valor_monetario"]
    result = _desambiguar_valores(nomes, df)
    assert result[1] == "valor_investimento"  # b = 1M
    assert result[2] == "valor_repasse"  # c = 500
    assert result[0] == "valor_contrapartida"  # a = 100


# ─── Desambiguacao: UH ────────────────────────────────────────────────────


def test_desambiguar_uh_two() -> None:
    df = pd.DataFrame(
        {
            "a": ["500"] * 10,
            "b": ["300"] * 10,
        }
    )
    nomes = ["qtd_uh", "qtd_uh"]
    result = _desambiguar_uh(nomes, df)
    assert result[0] == "qtd_uh_contratadas"  # maior
    assert result[1] == "qtd_uh_concluidas"  # menor


def test_desambiguar_codigos() -> None:
    nomes = ["codigo", "codigo", None, "codigo"]
    result = _desambiguar_codigos(nomes)
    assert result[0] == "codigo_1"
    assert result[1] == "codigo_2"
    assert result[2] is None
    assert result[3] == "codigo_3"


# ─── Integration: full inference on real tables ───────────────────────────


def test_inferir_pj_table() -> None:
    """Testa inferencia na tabela real bb_2019_2019_05_07_2019_05_07_pj."""
    df = carregar_amostra("bb_2019_2019_05_07_2019_05_07_pj")
    nomes, note = inferir_nomes_colunas(df)

    assert len(nomes) == 21
    assert "alta confianca" in note

    # Colunas chave devem ser detectadas
    assert "frente" in nomes
    assert "cnpj" in nomes
    assert "situacao_obra" in nomes
    assert "nome_empreendimento" in nomes
    assert "endereco" in nomes
    assert "nome_empresa" in nomes
    assert "data_inicio" in nomes
    assert "data_fim" in nomes
    assert "data_referencia" in nomes
    assert "valor_percentual_conclusao" in nomes

    # Colunas nao devem ter nomes genericos
    for nome in nomes:
        assert not nome.startswith("col_"), f"Coluna generica: {nome}"


def test_inferir_pf_pf_table() -> None:
    """Testa inferencia na tabela bb_2019_2019_05_07_pf_pf."""
    df = carregar_amostra("bb_2019_2019_05_07_pf_pf")
    nomes, note = inferir_nomes_colunas(df)

    assert len(nomes) == len(df.columns)
    assert len(nomes) == 15
    assert "alta confianca" in note
    assert "data_referencia" in nomes
    assert "data_inicio" in nomes


def test_inferir_pj_pf_table() -> None:
    """Testa inferencia na tabela bb_2019_2019_05_07_pj_pf."""
    df = carregar_amostra("bb_2019_2019_05_07_pj_pf")
    nomes, note = inferir_nomes_colunas(df)

    assert len(nomes) == 13
    assert "alta confianca" in note
    assert "data_referencia" in nomes
    assert "data_inicio" in nomes


# ─── Integration: tratar_sem_cabecalho ────────────────────────────────────


def test_tratar_sem_cabecalho_with_inference() -> None:
    """tratar_sem_cabecalho com inferir_nomes=True deve gerar warning."""
    from classificacao.tratamento_especiais import tratar_sem_cabecalho

    df = carregar_amostra("bb_2019_2019_05_07_2019_05_07_pj")
    df_t, info = tratar_sem_cabecalho(
        "bb_2019_2019_05_07_2019_05_07_pj", df, inferir_nomes=True
    )
    assert df_t is not None
    assert len(df_t) > 0
    warnings = info.get("warnings", [])
    assert any("inferidos" in w for w in warnings)


def test_tratar_sem_cabecalho_without_inference() -> None:
    """tratar_sem_cabecalho com inferir_nomes=False deve usar col_N."""
    from classificacao.tratamento_especiais import tratar_sem_cabecalho

    df = carregar_amostra("bb_2019_2019_05_07_2019_05_07_pj")
    df_t, info = tratar_sem_cabecalho(
        "bb_2019_2019_05_07_2019_05_07_pj", df, inferir_nomes=False
    )
    warnings = info.get("warnings", [])
    assert any("desabilitada" in w for w in warnings)


def test_tratar_sem_cabecalho_default_is_inference() -> None:
    """Sem especificar inferir_nomes, deve inferir por default."""
    from classificacao.tratamento_especiais import tratar_sem_cabecalho

    df = carregar_amostra("bb_2019_2019_05_07_2019_05_07_pj")
    df_t, info = tratar_sem_cabecalho("bb_2019_2019_05_07_2019_05_07_pj", df)
    warnings = info.get("warnings", [])
    assert any("inferidos" in w for w in warnings)


# ─── Disambiguation: synthetic multi-column ───────────────────────────────


def test_synthetic_multi_date() -> None:
    """DataFrame sintetico com 3 colunas de data."""
    df = pd.DataFrame(
        {
            "a": ["2013-01-01", "2014-01-01", "2015-01-01"] * 5,
            "b": ["2016-01-01", "2017-01-01", "2018-01-01"] * 5,
            "c": ["2019-05-30"] * 15,
        }
    )
    nomes, _ = inferir_nomes_colunas(df)
    assert nomes[0] == "data_inicio"
    assert nomes[1] == "data_fim"
    assert nomes[2] == "data_referencia"


def test_synthetic_multi_valor() -> None:
    """DataFrame sintetico com 4 colunas de valor monetario (valores > 100)."""
    df = pd.DataFrame(
        {
            "a": ["1000,00"] * 10,
            "b": ["5000,00"] * 10,
            "c": ["2000,00"] * 10,
            "d": ["200,00"] * 10,
        }
    )
    nomes, _ = inferir_nomes_colunas(df)
    assert nomes[1] == "valor_investimento"  # 5000
    assert nomes[2] == "valor_repasse"  # 2000
    assert nomes[0] == "valor_contrapartida"  # 1000
    assert "valor_monetario_1" in nomes[3]  # 200


def test_synthetic_multi_uh() -> None:
    """DataFrame sintetico com 3 colunas de UH."""
    df = pd.DataFrame(
        {
            "a": ["500"] * 10,
            "b": ["200"] * 10,
            "c": ["400"] * 10,
        }
    )
    nomes, _ = inferir_nomes_colunas(df)
    assert nomes[0] == "qtd_uh_contratadas"  # maior
    assert nomes[1] == "qtd_uh_concluidas"  # menor
    assert nomes[2] == "qtd_uh_entregues"  # intermediaria


def test_synthetic_partial_inference() -> None:
    """Algumas colunas inferidas, outras genericas."""
    df = pd.DataFrame(
        {
            "a": ["FAR"] * 20,
            "b": ["xyz_unknown_pattern_123"] * 20,
        }
    )
    nomes, note = inferir_nomes_colunas(df)
    assert nomes[0] == "frente"
    assert nomes[1] == "col_1"
    assert "parcialmente" in note


def test_synthetic_no_inference() -> None:
    """Nenhuma coluna atinge threshold."""
    df = pd.DataFrame(
        {
            "a": ["abc"] * 20,
            "b": ["def"] * 20,
        }
    )
    nomes, note = inferir_nomes_colunas(df)
    assert nomes[0] == "col_0"
    assert nomes[1] == "col_1"
    assert "sem exito" in note
