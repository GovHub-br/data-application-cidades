"""Testes unitários das regras R1–R7 e da heurística R3b.

Cobre cada regra isoladamente com fixtures sintéticas e valida o orquestrador
``classificar_formacao`` em cada categoria (task 4.2 e 8.1).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from classificacao.carregamento import carregar_csv
from classificacao.profiling import (
    DATA_VALUE,
    REAL_NAME,
    UNNAMED,
    inspecionar_linhas_cabecalho,
    profile_estrutural,
)
from classificacao.regras import (
    AMBIGUOUS,
    HEADER_IS_DATA,
    HEADER_IS_REAL,
    BEM_FORMADA,
    CABECALHO_COMPOSTO_1,
    CABECALHO_COMPOSTO_2,
    CABECALHO_PRIMEIRA_LINHA_1,
    CABECALHO_PRIMEIRA_LINHA_2,
    CABECALHO_SEGUNDA_LINHA,
    CABECALHO_TERCEIRA_LINHA_1,
    CABECALHO_TERCEIRA_LINHA_2,
    DADOS_SEM_UTILIDADE,
    NAO_COLUNARES_TIPO1,
    SEM_CABECALHO,
    SEPARADOR_PIPE,
    SUB_TABELAS_1,
    SUB_TABELAS_2,
    SUB_TABELAS_3,
    SUB_TABELAS_4,
    VAZIA,
    classificar_cabecalho_composto,
    classificar_cabecalho_deslocado,
    classificar_formacao,
    classificar_sub_tabelas,
    r1_vazia,
    r2_dados_sem_utilidade,
    r3_separador_pipe,
    r3b_decisao_cabecalho,
    r4_consistencia_tipos,
    r5_nao_colunares,
    r5_sub_tabelas_1,
    r5_sub_tabelas_2,
    r5_sub_tabelas_3,
    r5_sub_tabelas_4,
    r6_cabecalho_composto_1,
    r6_cabecalho_composto_2,
    r6_cabecalho_linhas,
)

# Aliases para compatibilidade com nomes antigos (remover em task futura)
CABECALHO_PRIMEIRA_LINHA = CABECALHO_PRIMEIRA_LINHA_1
CABECALHO_TERCEIRA_LINHA = CABECALHO_TERCEIRA_LINHA_1

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> pd.DataFrame:
    return carregar_csv(FIXTURES / f"{name}.csv")


def _classify(name: str) -> tuple[str, str, str]:
    df = _fixture(name)
    prof = profile_estrutural(df, file_size=(FIXTURES / f"{name}.csv").stat().st_size)
    return classificar_formacao(name, df, prof)


# --- R3b ---------------------------------------------------------------------


def test_r3b_tudo_data_value_eh_header_is_data() -> None:
    assert r3b_decisao_cabecalho([DATA_VALUE] * 5) == HEADER_IS_DATA


def test_r3b_tudo_real_name_eh_header_is_real() -> None:
    assert r3b_decisao_cabecalho([REAL_NAME] * 5) == HEADER_IS_REAL


def test_r3b_misto_eh_ambiguous() -> None:
    # 1 real + 4 unnamed: real_ratio=0.2, data_ratio=0 → ambíguo
    assert (
        r3b_decisao_cabecalho([REAL_NAME, UNNAMED, UNNAMED, UNNAMED, UNNAMED])
        == AMBIGUOUS
    )


def test_r3b_sem_colunas_eh_ambiguous() -> None:
    assert r3b_decisao_cabecalho([]) == AMBIGUOUS


# --- R1 ---------------------------------------------------------------------


def test_r1_vazia_pequena_uma_coluna() -> None:
    assert r1_vazia(11, 1) is True


def test_r1_nao_vazia_grande() -> None:
    assert r1_vazia(20_000, 1) is False


def test_r1_nao_vazia_muitas_colunas() -> None:
    assert r1_vazia(100, 5) is False


# --- R2 ---------------------------------------------------------------------


def test_r2_padroes_conhecidos() -> None:
    assert r2_dados_sem_utilidade("bb_2015_08_agosto_loginfesta") is True
    assert (
        r2_dados_sem_utilidade("bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados")
        is True
    )
    assert r2_dados_sem_utilidade("caixa_002_2018_novo_relat_rio_executivo") is True


def test_r2_tab_arquivos_dados_com_pipe_nao_descarta() -> None:
    df = pd.DataFrame({"registro": ["2023-01-01|123|responsavel|email@gov.br"]})
    assert (
        r2_dados_sem_utilidade(
            "bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados", df
        )
        is False
    )
    prof = profile_estrutural(df, file_size=10_000)
    formacao, conf, _notes = classificar_formacao(
        "bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados", df, prof
    )
    assert formacao == SEPARADOR_PIPE
    assert conf == "high"


def test_r2_nome_normal() -> None:
    assert r2_dados_sem_utilidade("bb_2019_2019_05_07_pf_pf") is False


# --- R4 ---------------------------------------------------------------------


def test_r4_bem_formada_consistente() -> None:
    assert r4_consistencia_tipos(_fixture("bem_formada")) is True


def test_r4_coluna_inconsistente() -> None:
    # Coluna com mistura forte de número e texto (>10% fora do dominante)
    df = pd.DataFrame({"a": ["1", "2", "3", "4", "5", "6", "7", "8", "texto", "outro"]})
    assert r4_consistencia_tipos(df) is False


# --- R5 ---------------------------------------------------------------------


def test_r5_nao_colunares_disperso() -> None:
    assert r5_nao_colunares(0.5, [UNNAMED] * 7) is True


def test_r5_nao_colunares_denso() -> None:
    assert r5_nao_colunares(0.05, [UNNAMED] * 7) is False


def test_r5_com_coluna_real_nao_eh_nao_colunar() -> None:
    assert r5_nao_colunares(0.5, [REAL_NAME, UNNAMED, UNNAMED]) is False


# --- R6 ---------------------------------------------------------------------


def test_r6_cabecalho_na_primeira_linha() -> None:
    insp = inspecionar_linhas_cabecalho(_fixture("cabecalho_na_primeira_linha_1"))
    assert r6_cabecalho_linhas(insp) == CABECALHO_PRIMEIRA_LINHA_1


def test_r6_cabecalho_na_terceira_linha() -> None:
    insp = inspecionar_linhas_cabecalho(_fixture("cabecalho_na_terceira_linha_1"))
    assert r6_cabecalho_linhas(insp) == CABECALHO_TERCEIRA_LINHA_1


def test_r6_bem_formada_sem_cabecalho_deslocado() -> None:
    insp = inspecionar_linhas_cabecalho(_fixture("bem_formada"))
    assert r6_cabecalho_linhas(insp) is None


# --- classificar_formacao (end-to-end por fixture) --------------------------


def test_classificar_bem_formada() -> None:
    assert _classify("bem_formada") == (BEM_FORMADA, "high", "")


def test_classificar_sem_cabecalho() -> None:
    assert _classify("sem_cabecalho") == (SEM_CABECALHO, "high", "")


def test_classificar_cabecalho_na_primeira_linha() -> None:
    assert _classify("cabecalho_na_primeira_linha_1") == (
        CABECALHO_PRIMEIRA_LINHA_1,
        "high",
        "",
    )


def test_classificar_cabecalho_na_terceira_linha() -> None:
    assert _classify("cabecalho_na_terceira_linha_1") == (
        CABECALHO_TERCEIRA_LINHA_1,
        "high",
        "",
    )


def test_classificar_nao_colunares_tipo1() -> None:
    assert _classify("nao_colunares_tipo1") == (
        NAO_COLUNARES_TIPO1,
        "low",
        "R8: nenhuma regra R1-R7 match, revisar manualmente",
    )


def test_classificar_vazia() -> None:
    assert _classify("vazia") == (VAZIA, "high", "")


def test_classificar_dados_sem_utilidade_por_nome() -> None:
    # A fixture "dados_sem_utilidade" não tem padrão no nome; testamos R2 pelo
    # nome real que contém o padrão "loginfesta".
    df = _fixture("dados_sem_utilidade")
    prof = profile_estrutural(df, file_size=53)
    form, _, _ = classificar_formacao("bb_2015_08_agosto_loginfesta", df, prof)
    assert form == DADOS_SEM_UTILIDADE


# --- R3: Separador pipe -------------------------------------------------------


def test_r3_separador_pipe_positivo() -> None:
    """Tabela com | nos dados deve ser detectada."""
    df = _fixture("separador_|")
    assert r3_separador_pipe(df) is True


def test_r3_separador_pipe_negativo() -> None:
    """Tabela sem | nos dados não deve ser detectada."""
    df = _fixture("bem_formada")
    assert r3_separador_pipe(df) is False


# --- R5: Sub-tabelas ----------------------------------------------------------


def test_r5_sub_tabelas_1_positivo() -> None:
    df = _fixture("sub_tabelas_1")
    assert r5_sub_tabelas_1(df) is True


def test_r5_sub_tabelas_1_negativo() -> None:
    df = _fixture("bem_formada")
    assert r5_sub_tabelas_1(df) is False


def test_r5_sub_tabelas_2_positivo() -> None:
    df = _fixture("sub_tabelas_2")
    classif = [UNNAMED] * len(df.columns)
    assert r5_sub_tabelas_2(df, classif) is True


def test_r5_sub_tabelas_2_negativo() -> None:
    df = _fixture("bem_formada")
    classif = [REAL_NAME] * len(df.columns)
    assert r5_sub_tabelas_2(df, classif) is False


def test_r5_sub_tabelas_3_positivo() -> None:
    from classificacao.profiling import classificar_colunas

    df = _fixture("sub_tabelas_3")
    classif = classificar_colunas(df)
    assert r5_sub_tabelas_3(df, classif) is True


def test_r5_sub_tabelas_3_negativo() -> None:
    from classificacao.profiling import classificar_colunas

    df = _fixture("bem_formada")
    classif = classificar_colunas(df)
    assert r5_sub_tabelas_3(df, classif) is False


def test_r5_sub_tabelas_4_positivo() -> None:
    df = _fixture("sub_tabelas_4")
    classif = [UNNAMED] * len(df.columns)
    assert r5_sub_tabelas_4(df, classif) is True


def test_r5_sub_tabelas_4_negativo() -> None:
    df = _fixture("bem_formada")
    classif = [REAL_NAME] * len(df.columns)
    assert r5_sub_tabelas_4(df, classif) is False


def test_classificar_sub_tabelas() -> None:
    """Dispatcher testa todos os 4 tipos na ordem correta."""
    from classificacao.profiling import classificar_colunas

    # sub_tabelas_1
    df1 = _fixture("sub_tabelas_1")
    assert classificar_sub_tabelas(df1, []) == SUB_TABELAS_1
    # sub_tabelas_2
    df2 = _fixture("sub_tabelas_2")
    c2 = [UNNAMED] * len(df2.columns)
    assert classificar_sub_tabelas(df2, c2) == SUB_TABELAS_2
    # sub_tabelas_3
    df3 = _fixture("sub_tabelas_3")
    c3 = classificar_colunas(df3)
    assert classificar_sub_tabelas(df3, c3) == SUB_TABELAS_3
    # sub_tabelas_4
    df4 = _fixture("sub_tabelas_4")
    c4 = [UNNAMED] * len(df4.columns)
    assert classificar_sub_tabelas(df4, c4) == SUB_TABELAS_4
    # None
    df5 = _fixture("bem_formada")
    c5 = classificar_colunas(df5)
    assert classificar_sub_tabelas(df5, c5) is None


# --- R6: Cabeçalho composto ---------------------------------------------------


def test_r6_cabecalho_composto_1_positivo() -> None:
    from classificacao.profiling import classificar_colunas

    df = _fixture("cabecalho_composto_1")
    classif = classificar_colunas(df)
    assert r6_cabecalho_composto_1(df, classif) is True


def test_r6_cabecalho_composto_1_negativo() -> None:
    from classificacao.profiling import classificar_colunas

    df = _fixture("bem_formada")
    classif = classificar_colunas(df)
    assert r6_cabecalho_composto_1(df, classif) is False


def test_r6_cabecalho_composto_2_positivo() -> None:
    from classificacao.profiling import classificar_colunas

    df = _fixture("cabecalho_composto_2")
    classif = classificar_colunas(df)
    assert r6_cabecalho_composto_2(df, classif) is True


def test_r6_cabecalho_composto_2_negativo() -> None:
    from classificacao.profiling import classificar_colunas

    df = _fixture("bem_formada")
    classif = classificar_colunas(df)
    assert r6_cabecalho_composto_2(df, classif) is False


def test_classificar_cabecalho_composto() -> None:
    from classificacao.profiling import classificar_colunas

    df1 = _fixture("cabecalho_composto_1")
    c1 = classificar_colunas(df1)
    assert classificar_cabecalho_composto(df1, c1) == CABECALHO_COMPOSTO_1
    df2 = _fixture("cabecalho_composto_2")
    c2 = classificar_colunas(df2)
    assert classificar_cabecalho_composto(df2, c2) == CABECALHO_COMPOSTO_2
    df3 = _fixture("bem_formada")
    c3 = classificar_colunas(df3)
    assert classificar_cabecalho_composto(df3, c3) is None


# --- R7: Cabeçalho deslocado (6 subtipos) ------------------------------------


def test_cabecalho_deslocado_primeira_linha_1() -> None:
    df = _fixture("cabecalho_na_primeira_linha_1")
    assert classificar_cabecalho_deslocado(df) == CABECALHO_PRIMEIRA_LINHA_1


def test_cabecalho_deslocado_primeira_linha_2() -> None:
    df = _fixture("cabecalho_na_primeira_linha_2")
    result = classificar_cabecalho_deslocado(df)
    assert result == CABECALHO_PRIMEIRA_LINHA_2


def test_cabecalho_deslocado_segunda_linha() -> None:
    df = _fixture("cabecalho_na_segunda_linha")
    result = classificar_cabecalho_deslocado(df)
    assert result == CABECALHO_SEGUNDA_LINHA


def test_cabecalho_deslocado_terceira_linha_1() -> None:
    df = _fixture("cabecalho_na_terceira_linha_1")
    result = classificar_cabecalho_deslocado(df)
    assert result == CABECALHO_TERCEIRA_LINHA_1


def test_cabecalho_deslocado_terceira_linha_2() -> None:
    df = _fixture("cabecalho_na_terceira_linha_2")
    result = classificar_cabecalho_deslocado(df)
    assert result == CABECALHO_TERCEIRA_LINHA_2


def test_classificar_deslocado_bem_formada_retorna_none() -> None:
    """Tabela bem formada não deve ser detectada como cabeçalho deslocado."""
    df = _fixture("bem_formada")
    assert classificar_cabecalho_deslocado(df) is None


# --- classificar_formacao end-to-end com novos fixtures -----------------------


def test_classificar_formacao_separador_pipe() -> None:
    fn, conf, notes = _classify("separador_|")
    assert fn == SEPARADOR_PIPE
    assert conf == "high"


def test_classificar_formacao_sub_tabelas_1() -> None:
    fn, conf, notes = _classify("sub_tabelas_1")
    assert fn == SUB_TABELAS_1
    assert conf == "high"


def test_classificar_formacao_sub_tabelas_2() -> None:
    fn, conf, notes = _classify("sub_tabelas_2")
    assert fn == SUB_TABELAS_2
    assert conf == "high"


def test_classificar_formacao_sub_tabelas_3() -> None:
    fn, conf, notes = _classify("sub_tabelas_3")
    assert fn == SUB_TABELAS_3
    assert conf == "high"


def test_classificar_formacao_sub_tabelas_4() -> None:
    fn, conf, notes = _classify("sub_tabelas_4")
    assert fn == SUB_TABELAS_4
    assert conf == "medium"  # sub_tabelas_4 has medium confidence


def test_classificar_formacao_cabecalho_composto_1() -> None:
    fn, conf, notes = _classify("cabecalho_composto_1")
    assert fn == CABECALHO_COMPOSTO_1
    assert conf == "high"


def test_classificar_formacao_cabecalho_composto_2() -> None:
    fn, conf, notes = _classify("cabecalho_composto_2")
    assert fn == CABECALHO_COMPOSTO_2
    assert conf == "medium"


def test_classificar_formacao_cabecalho_primeira_linha_2() -> None:
    fn, conf, notes = _classify("cabecalho_na_primeira_linha_2")
    assert fn == CABECALHO_PRIMEIRA_LINHA_2
    assert conf == "high"


def test_classificar_formacao_cabecalho_segunda_linha() -> None:
    fn, conf, notes = _classify("cabecalho_na_segunda_linha")
    assert fn == CABECALHO_SEGUNDA_LINHA
    assert conf == "high"


def test_classificar_formacao_cabecalho_terceira_linha_2() -> None:
    fn, conf, notes = _classify("cabecalho_na_terceira_linha_2")
    assert fn == CABECALHO_TERCEIRA_LINHA_2
    assert conf == "high"
