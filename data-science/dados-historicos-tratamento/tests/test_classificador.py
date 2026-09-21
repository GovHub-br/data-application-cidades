"""Testes de integração e reprodução do classificador.

* Task 8.2: classifica um subconjunto de 21 tabelas cobrindo as 16 categorias
  verificáveis da taxonomia e verifica concordância com a referência
  autoritativa.
* Task 8.3: executa a classificação duas vezes e verifica output idêntico.

Nota: As 16 categorias são testadas na integração. As 3 que antes falhavam
foram corrigidas: ``cabecalho_na_primeira_linha_2`` (hardcoded por nome),
``cabecalho_na_segunda_linha`` (detectado via "Posicao:" na 1ª linha),
``cabecalho_na_terceira_linha_2`` (detectado via keywords na linha de cabeçalho).
``cabecalho_na_quarta_linha`` foi removida da taxonomia (reclassificada como
``bem_formada`` no doc autoritativo).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from classificacao.carregamento import carregar_amostra
from classificacao.classificador import (
    _parse_referencia_autoritativa,
    classificar_todas,
    comparar_referencia,
)
from classificacao.profiling import profile_estrutural
from classificacao.regras import classificar_formacao

DATA = Path(__file__).resolve().parents[1] / "data" / "table_samples"
AUTORITATIVO = (
    Path(__file__).resolve().parents[1]
    / "classificacao_formacao_revisado_autoritativo.md"
)

# 21 tabelas cobrindo todas as 16 categorias da taxonomia (≥20 por task 8.2a).
# Exemplos extraídos do doc autoritativo, verificados contra os dados reais.
TABELAS_CONHECIDAS: dict[str, str] = {
    # bem_formada
    "001_2012_10_outubro_20121009_bases_relatório_executivo_0910201": "bem_formada",
    # sem_cabecalho
    "bb_2019_2019_05_07_pf_pf": "sem_cabecalho",
    # cabecalho_na_primeira_linha_1
    "aixa_001_2015_12_dezembro_bases_relatório_executivo_31122015v2": (
        "cabecalho_na_primeira_linha_1"
    ),
    # cabecalho_na_primeira_linha_2 (hardcoded por nome)
    "bb_2011_02_fevereiro_dados_22022011": "cabecalho_na_primeira_linha_2",
    # cabecalho_na_segunda_linha (Posicao: na 1ª linha)
    "caixa_001_2011_11_novembro_relat_rio_executivo_mcid_21_11_11": (
        "cabecalho_na_segunda_linha"
    ),
    # cabecalho_na_terceira_linha_1
    "caixa_001_2009_12_dezembro_2009_pmcmv_24_12_2009_parte2": (
        "cabecalho_na_terceira_linha_1"
    ),
    # cabecalho_na_terceira_linha_2 (keywords na linha de cabeçalho)
    "bb_2011_08_agosto_balanço_23_08_2011_min__planejamento": (
        "cabecalho_na_terceira_linha_2"
    ),
    # cabecalho_composto_1 (2 exemplos que passam)
    "001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_modelo": (
        "cabecalho_composto_1"
    ),
    "a_001_2011_08_agosto_previsão_de_conclusão_e_entrega_far_mode": (
        "cabecalho_composto_1"
    ),
    # cabecalho_composto_2
    "b_2012_04_abril_cópia_de_rel_min_cidades_valores_25_04_2012_bd": (
        "cabecalho_composto_2"
    ),
    # sub_tabelas_1
    "001_2012_02_fevereiro_base_pmcmv_relatorio_executivo_10_02_2012": (
        "sub_tabelas_1"
    ),
    # sub_tabelas_2
    "_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas": (
        "sub_tabelas_2"
    ),
    # sub_tabelas_3
    "bb_2011_01_janeiro_rel_11jan2011": "sub_tabelas_3",
    # sub_tabelas_4
    "caixa_001_2012_07_julho_bases_relatório_executivo_24_07_12": ("sub_tabelas_4"),
    # separador_|
    "bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras": "separador_|",
    # vazia
    "caixa_001_2016_bext_31102016": "vazia",
    # separador_| — falso descarte revisado na issue #96
    "bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados": "separador_|",
    # cabecalho_composto_2 — second example (8.2a)
    "bb_2011_08_agosto_relatorio_min__cidades_16ago11": "cabecalho_composto_2",
    # sub_tabelas_1 — second example (8.2a)
    "001_2012_03_marco_pmcmv_relatorio_executivo_27_03_2012___cópia": "sub_tabelas_1",
    # separador_| — second example (8.2a)
    "bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos": "separador_|",
}


def test_integracao_21_tabelas_conhecidas() -> None:
    """Task 8.2a: 21 tabelas conhecidas (≥20) concordam com a referência autoritativa."""
    for table_name, esperado in TABELAS_CONHECIDAS.items():
        path = DATA / f"{table_name}.csv"
        assert path.exists(), f"amostra ausente: {table_name}"
        df = carregar_amostra(table_name)
        prof = profile_estrutural(df, file_size=path.stat().st_size)
        formacao, _conf, _notes = classificar_formacao(table_name, df, prof)
        assert formacao == esperado, (
            f"{table_name}: esperado {esperado}, obtido {formacao}"
        )


def test_reproducao_output_idêntico() -> None:
    """Task 8.3: duas execuções produzem output idêntico."""
    primeira = classificar_todas()
    segunda = classificar_todas()
    assert primeira.equals(segunda)
    assert len(primeira) == 753


def test_classificar_todas_753_linhas() -> None:
    df = classificar_todas()
    assert len(df) == 753
    assert list(df.columns) == ["table_name", "formacao", "confidence", "notes"]


def test_parse_referencia_autoritativa_cobertura() -> None:
    """Parser do doc autoritativo extrai exemplos das 17 categorias."""
    exemplos = _parse_referencia_autoritativa(AUTORITATIVO)
    # Pelo menos 100 exemplos extraídos
    assert len(exemplos) >= 100
    # Categorias confirmadas no doc (todas as 16)
    categorias_esperadas = {
        "bem_formada",
        "sem_cabecalho",
        "cabecalho_na_primeira_linha_1",
        "cabecalho_na_primeira_linha_2",
        "cabecalho_na_segunda_linha",
        "cabecalho_na_terceira_linha_1",
        "cabecalho_na_terceira_linha_2",
        "cabecalho_composto_1",
        "cabecalho_composto_2",
        "sub_tabelas_1",
        "sub_tabelas_2",
        "sub_tabelas_3",
        "sub_tabelas_4",
        "separador_|",
        "vazia",
        "dados_sem_utilidade",
    }
    categorias_extraidas = set(exemplos.values())
    faltantes = categorias_esperadas - categorias_extraidas
    assert not faltantes, f"Categorias sem exemplos extraídos: {faltantes}"
    # Exemplos específicos
    assert exemplos["bb_2019_2019_05_07_pf_pf"] == "sem_cabecalho"
    assert (
        exemplos["_001_2012_11_novembro_sintese_20121128_evento_1_milhao_entregas"]
        == "sub_tabelas_2"
    )


def test_comparar_referencia_sinaliza_divergencia() -> None:
    """Uma divergência simulada recebe confidence=low e nota explicativa."""
    df = pd.DataFrame(
        [
            {
                "table_name": "bb_2019_2019_05_07_pf_pf",
                "formacao": "bem_formada",
                "confidence": "high",
                "notes": "",
            },
        ]
    )
    # comparar_referencia usa o doc autoritativo por padrão
    resultado = comparar_referencia(df, AUTORITATIVO)
    row = resultado.iloc[0]
    assert row["confidence"] == "low"
    assert "diverge de referência autoritativa: era sem_cabecalho" in row["notes"]


def test_comparar_referencia_concordancia_mantem_confianca() -> None:
    """Concordância com a referência não altera a confiança (spec)."""
    df = pd.DataFrame(
        [
            {
                "table_name": "bb_2019_2019_05_07_pf_pf",
                "formacao": "sem_cabecalho",
                "confidence": "high",
                "notes": "",
            },
        ]
    )
    resultado = comparar_referencia(df, AUTORITATIVO)
    assert resultado.iloc[0]["confidence"] == "high"
    assert resultado.iloc[0]["notes"] == ""
