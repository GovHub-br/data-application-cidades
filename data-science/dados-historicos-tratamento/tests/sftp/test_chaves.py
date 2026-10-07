"""Testes para identificação de chaves candidatas."""

from __future__ import annotations

import re

import pandas as pd
from sftp.matching import CHAVE_PATTERNS, identificar_chaves_candidatas


class TestChavePatterns:
    def test_todos_patterns_compilam(self) -> None:
        for pattern_str, label in CHAVE_PATTERNS:
            pat = re.compile(pattern_str)
            assert pat is not None, f"Pattern {label} não compila: {pattern_str}"

    def test_cpf_pattern(self) -> None:
        pat = re.compile(r"\bcpf\b")
        # \b não quebra em _, então só funciona com espaços/início/fim
        assert pat.search("cpf")
        assert pat.search(" cpf ")
        assert pat.search("cpf ")
        assert not pat.search("cpf_cnpj")
        # Em colunas normalizadas, underscore é separador comum
        # O padrão real em CHAVE_PATTERNS usa \b, que não quebra em _
        # Isso significa que só captura "cpf" isolado ou no início/fim

    def test_cnpj_pattern(self) -> None:
        pat = re.compile(r"\bcnpj\b")
        assert pat.search("cnpj")
        assert pat.search(" cnpj ")
        assert pat.search("cnpj ")

    def test_anomes_pattern(self) -> None:
        pat = re.compile(r"\banomes\b")
        assert pat.search("anomes")
        assert pat.search(" anomes ")
        assert not pat.search("anomes_nu_apf")

    def test_contrato_pattern(self) -> None:
        pat = re.compile(r"\bcontrato\b")
        assert pat.search("contrato")
        assert pat.search(" contrato ")

    def test_ibge_pattern(self) -> None:
        pat = re.compile(r"\bibge_no\b")
        assert pat.search("ibge_no")
        assert pat.search(" ibge_no ")
        assert not pat.search("ibge_nome")

    def test_uh_contratados_pattern(self) -> None:
        pat = re.compile(r"\buh_contratados\b")
        assert pat.search("uh_contratados")
        assert pat.search(" uh_contratados ")


class TestIdentificarChavesCandidatasEdgeCases:
    def test_dataframe_vazio(self) -> None:
        df_campos = pd.DataFrame(
            columns=[
                "tabela_sftp",
                "tabela_dump",
                "campo",
                "tipo_sftp",
                "tipo_dump",
                "match_tipo",
            ]
        )
        df_chaves = identificar_chaves_candidatas(df_campos)
        assert df_chaves.empty

    def test_sem_chaves_conhecidas(self) -> None:
        df_campos = pd.DataFrame(
            {
                "tabela_sftp": ["t1", "t1"],
                "tabela_dump": ["d1", "d1"],
                "campo": ["coluna_x", "coluna_y"],
                "tipo_sftp": ["text", "text"],
                "tipo_dump": ["text", "text"],
                "match_tipo": ["exato", "exato"],
            }
        )
        df_chaves = identificar_chaves_candidatas(df_campos)
        # Nenhuma coluna bate com CHAVE_PATTERNS → vazio
        assert df_chaves.empty

    def test_multiplas_ocorrencias_mesma_chave(self) -> None:
        df_campos = pd.DataFrame(
            {
                "tabela_sftp": ["t1", "t2", "t3", "t4", "t5"],
                "tabela_dump": ["d1", "d2", "d3", "d4", "d5"],
                "campo": ["cpf", "cpf", "cpf", "cod", "cod"],
                "tipo_sftp": ["text"] * 5,
                "tipo_dump": ["text"] * 5,
                "match_tipo": ["exato"] * 5,
            }
        )
        df_chaves = identificar_chaves_candidatas(df_campos)
        row_cpf = df_chaves[df_chaves["chave"] == "cpf"]
        assert row_cpf["qtd_tabelas_sftp"].iloc[0] == 3
        assert row_cpf["qtd_tabelas_dump"].iloc[0] == 3
        row_cod = df_chaves[df_chaves["chave"] == "cod"]
        assert row_cod["qtd_tabelas_sftp"].iloc[0] == 2
