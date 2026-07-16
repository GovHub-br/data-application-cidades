"""Testes para src/sftp/normalizacao.py."""

from __future__ import annotations

from sftp.normalizacao import (
    canonicalizar_stem,
    normalizar_nome_coluna,
    extrair_anos,
    extrair_prefixo_institucional,
)


class TestCanonicalizarStem:
    """Casos do design D2."""

    def test_gefus_anteriores_snh_pmcmv(self) -> None:
        assert (
            canonicalizar_stem(
                "gefus_anteriores_202408_snh_pmcmv_dados_prioritarios_af_caixa"
            )
            == "snh_pmcmv_dados_prioritarios"
        )

    def test_o_recente_snh_pmcmv(self) -> None:
        stem = canonicalizar_stem(
            "o_recente_202406_snh_pmcmv_dados_prioritarios_af_caixa_entregas"
        )
        # Remove prefix o_recente_, suffix _entregas
        assert "snh_pmcmv_dados_prioritarios" in stem
        assert "o_recente" not in stem
        assert "entregas" not in stem.lower()

    def test_caixa_nnn_relatorio(self) -> None:
        assert (
            canonicalizar_stem(
                "caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2"
            )
            == "fevereiro_relatorio_cidades"
        )

    def test_bb_nnn_bext(self) -> None:
        assert canonicalizar_stem("bb_003_2017_bext_out2017") == "bext_out2017"

    def test_gefus_com_mes(self) -> None:
        assert (
            canonicalizar_stem(
                "gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un"
            )
            == "snh_pmcmv_dados_prioritarios_da_entrega_da_un"
        )

    def test_intnnn_ministerio(self) -> None:
        stem = canonicalizar_stem("int058_ministeriocidades_pnhr_bb_pf_20231228")
        # Deve remover prefixo intNNN_ e sufixo _YYYYMMDD
        assert "pnhr" in stem
        assert "int058" not in stem

    def test_idempotencia(self) -> None:
        nomes = [
            "gefus_anteriores_202408_snh_pmcmv_dados_prioritarios_af_caixa",
            "caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2",
            "bb_003_2017_bext_out2017",
            "gefus_202601_snh_pmcmv_dados_prioritarios_da_entrega_da_un",
            "int058_ministeriocidades_pnhr_bb_pf_20231228",
        ]
        for nome in nomes:
            first = canonicalizar_stem(nome)
            second = canonicalizar_stem(first)
            assert first == second, (
                f"Idempotência falhou para: {nome} -> {first} -> {second}"
            )

    def test_nome_ja_normalizado(self) -> None:
        assert (
            canonicalizar_stem("snh_pmcmv_dados_prioritarios")
            == "snh_pmcmv_dados_prioritarios"
        )

    def test_underscores_duplicados(self) -> None:
        assert canonicalizar_stem("gefus___teste") == "teste"


class TestNormalizarNomeColuna:
    def test_maiusculo_acento(self) -> None:
        assert normalizar_nome_coluna("Dt_Nasc") == "dt_nasc"
        assert normalizar_nome_coluna("Número_Contrato") == "numero_contrato"

    def test_underscores_multiplos(self) -> None:
        assert normalizar_nome_coluna("nome___coluna") == "nome_coluna"


class TestExtrairAnos:
    def test_anos_no_nome(self) -> None:
        assert extrair_anos(
            "caixa_001_2016_02_fevereiro_relatorio_cidades_20160229_v2"
        ) == {2016}

    def test_multiplos_anos(self) -> None:
        result = extrair_anos("tabela_2021_a_2023")
        assert 2021 in result
        assert 2023 in result

    def test_sem_ano(self) -> None:
        assert extrair_anos("tabela_sem_ano") == set()


class TestExtrairPrefixoInstitucional:
    def test_caixa(self) -> None:
        assert (
            extrair_prefixo_institucional("caixa_001_2016_02_fevereiro_relatorio")
            == "caixa"
        )

    def test_bb(self) -> None:
        assert extrair_prefixo_institucional("bb_003_2017_bext_out2017") == "bb"

    def test_sem_prefixo(self) -> None:
        assert extrair_prefixo_institucional("gefus_anteriores_snh_pmcmv") is None
