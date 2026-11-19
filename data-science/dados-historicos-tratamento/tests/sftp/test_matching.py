"""Testes para src/sftp/matching.py."""

from __future__ import annotations

import pandas as pd
from sftp.matching import (
    jaccard_colunas,
    camada1_hash_exato,
    camada2_stem_canonico,
    camada3_jaccard_colunas,
    executar_batimento,
    identificar_chaves_candidatas,
)


class TestJaccardColunas:
    def test_iguais(self) -> None:
        assert jaccard_colunas({"a", "b", "c"}, {"a", "b", "c"}) == 1.0

    def test_disjuntos(self) -> None:
        assert jaccard_colunas({"a", "b"}, {"c", "d"}) == 0.0

    def test_parcial(self) -> None:
        # {a,b,c} ∩ {a,b,d} = 2, union = 4 → 0.5
        assert jaccard_colunas({"a", "b", "c"}, {"a", "b", "d"}) == 0.5

    def test_vazios(self) -> None:
        assert jaccard_colunas(set(), set()) == 0.0
        assert jaccard_colunas({"a"}, set()) == 0.0


class TestCamada1HashExato:
    def test_match_entre_schemas(self) -> None:
        index_hash = {
            "abc": {"sftp.tab1", "dados_historicos.tab1", "sftp.tab2"},
        }
        df = camada1_hash_exato(
            tabelas_sftp={"tab1", "tab2"},
            tabelas_dh={"tab1"},
            index_hash=index_hash,
        )
        assert len(df) == 2  # 2 pares cross-schema
        assert set(df["metodo"].unique()) == {"hash_exato"}
        assert set(df["confianca"].unique()) == {"alta"}

    def test_sem_match(self) -> None:
        index_hash = {
            "abc": {"sftp.tab1"},
        }
        df = camada1_hash_exato(
            tabelas_sftp={"tab1"},
            tabelas_dh={"tab1"},
            index_hash=index_hash,
        )
        # Ambas as tabelas existem mas não estão no mesmo grupo de hash
        assert df.empty


class TestCamada2StemCanonico:
    def test_stem_comum_com_jaccard(self) -> None:
        # Nomes que produzem o mesmo stem canônico
        colunas_sftp = {
            "gefus_anteriores_202408_dados_prioritarios": {"cpf", "nome", "contrato"}
        }
        colunas_dh = {
            "dados_prioritarios": {"cpf", "nome", "contrato"}
        }  # Jaccard = 1.0
        tabelas_sftp = {"gefus_anteriores_202408_dados_prioritarios"}
        tabelas_dh = {"dados_prioritarios"}

        df = camada2_stem_canonico(tabelas_sftp, tabelas_dh, colunas_sftp, colunas_dh)
        assert not df.empty
        assert df["metodo"].iloc[0] == "stem_canonico"
        assert df["confianca"].iloc[0] == "media"

    def test_stem_sem_colunas_compativeis(self) -> None:
        # Nomes que produzem o mesmo stem, mas colunas muito diferentes
        colunas_sftp = {"caixa_001_2017_dados_prioritarios": {"a", "b", "c"}}
        colunas_dh = {"dados_prioritarios": {"x", "y", "z"}}  # Jaccard = 0.0
        tabelas_sftp = {"caixa_001_2017_dados_prioritarios"}
        tabelas_dh = {"dados_prioritarios"}

        df = camada2_stem_canonico(tabelas_sftp, tabelas_dh, colunas_sftp, colunas_dh)
        # Jaccard < 0.3 → não registra
        assert df.empty


class TestCamada3Jaccard:
    def test_match_acima_limiar(self) -> None:
        colunas_sftp = {"tab_x": {"cpf", "nome", "contrato", "dt_nasc", "valor"}}
        colunas_dh = {"tab_y": {"cpf", "nome", "contrato", "dt_nascimento"}}
        # Jaccard: intersec={cpf,nome,contrato}=3, union=6 → 0.5

        df = camada3_jaccard_colunas({"tab_x"}, colunas_sftp, colunas_dh, limiar=0.5)
        assert not df.empty
        assert df["metodo"].iloc[0] == "jaccard_colunas"
        assert df["confianca"].iloc[0] == "baixa"

    def test_sem_match_abaixo_limiar(self) -> None:
        colunas_sftp = {"tab_x": {"a", "b"}}
        colunas_dh = {"tab_y": {"c", "d", "e", "f"}}  # Jaccard = 0.0

        df = camada3_jaccard_colunas({"tab_x"}, colunas_sftp, colunas_dh, limiar=0.5)
        assert df.empty


class TestIdentificarChavesCandidatas:
    def test_chaves_cpf(self) -> None:
        df_campos = pd.DataFrame(
            {
                "tabela_sftp": ["t1", "t2", "t3"],
                "tabela_dump": ["d1", "d1", "d2"],
                "campo": ["cpf", "cnpj", "cpf"],
                "tipo_sftp": ["text", "text", "text"],
                "tipo_dump": ["text", "text", "text"],
                "match_tipo": ["exato", "exato", "exato"],
            }
        )
        df_chaves = identificar_chaves_candidatas(df_campos)
        assert "cpf" in df_chaves["chave"].values
        assert "cnpj" in df_chaves["chave"].values

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


class TestExecutarBatimento:
    def test_pipeline_completo(self) -> None:
        """Testa o orquestrador completo com dados mínimos."""
        inv_sftp = pd.DataFrame(
            {
                "table_name": [
                    "tab_sftp_1",
                    "tab_sftp_1",
                    "tab_sftp_2",
                    "tab_sftp_2",
                ],
                "column_name": ["cpf", "nome", "cpf", "contrato"],
                "data_type": ["text", "text", "text", "text"],
            }
        )
        inv_dh = pd.DataFrame(
            {
                "table_name": [
                    "tab_dh_1",
                    "tab_dh_1",
                    "tab_dh_2",
                    "tab_dh_2",
                ],
                "column_name": ["cpf", "nome", "cpf", "contrato"],
                "data_type": ["text", "text", "text", "text"],
            }
        )
        est_sftp = pd.DataFrame({"tabela": ["tab_sftp_1", "tab_sftp_2"]})
        est_dh = pd.DataFrame({"tabela": ["tab_dh_1", "tab_dh_2"]})
        comp = pd.DataFrame(
            {
                "hash_estrutura": pd.Series(dtype=str),
                "quantidade_tabelas": pd.Series(dtype=float),
                "tabelas": pd.Series(dtype=str),
            }
        )

        result = executar_batimento(inv_sftp, inv_dh, est_sftp, est_dh, comp)
        # Com hash vazio, só camadas 2/3 operam
        assert not result.relacionadas.empty
        assert not result.campos_em_comum.empty
        assert not result.chaves.empty
        assert "relacionadas" in result._fields
        assert "chaves" in result._fields
