"""Testes para src/sftp/leitura_artefatos.py."""

from __future__ import annotations

import pytest
from pathlib import Path
from sftp.leitura_artefatos import (
    listar_artefatos,
    ler_inventario,
    ler_estrutura,
    ler_comparacao,
    indexar_por_hash,
)


class TestListarArtefatos:
    def test_tipo_invalido(self) -> None:
        with pytest.raises(ValueError, match="Tipo de artefato desconhecido"):
            listar_artefatos("tipo_inexistente")

    def test_diretorio_vazio(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            listar_artefatos("inventario_sftp", diretorio=tmp_path)

    def test_arquivo_encontrado(self, tmp_path: Path) -> None:
        # Criar arquivo com nome que bate no pattern
        artefato = tmp_path / "2026_inventario_tabelas_colunas_sftp.csv"
        artefato.write_text("table_name,column_name,data_type\n")
        result = listar_artefatos("inventario_sftp", diretorio=tmp_path)
        assert result == artefato

    def test_multiplos_arquivos_usar_mais_recente(self, tmp_path: Path) -> None:
        # Criar dois artefatos; o mais recente lexicalmente deve ser escolhido
        a1 = tmp_path / "2026_inventario_tabelas_colunas_sftp.csv"
        a2 = tmp_path / "2027_inventario_tabelas_colunas_sftp.csv"
        a1.write_text("table_name,column_name,data_type\n")
        a2.write_text("table_name,column_name,data_type\n")
        result = listar_artefatos("inventario_sftp", diretorio=tmp_path)
        assert result == a2


class TestLerInventario:
    def test_csv_valido(self, tmp_path: Path) -> None:
        p = tmp_path / "inventario.csv"
        p.write_text(
            "table_name,column_name,data_type\ntab1,col_a,text\ntab1,col_b,integer\n"
        )
        df = ler_inventario(p)
        assert list(df.columns) == ["table_name", "column_name", "data_type"]
        assert len(df) == 2
        assert df["table_name"].iloc[0] == "tab1"

    def test_csv_com_aspas_no_header(self, tmp_path: Path) -> None:
        p = tmp_path / "inventario_aspas.csv"
        p.write_text('"table_name","column_name","data_type"\ntab1,col_a,text\n')
        df = ler_inventario(p)
        assert list(df.columns) == ["table_name", "column_name", "data_type"]

    def test_csv_vazio_header_only(self, tmp_path: Path) -> None:
        p = tmp_path / "inventario_vazio.csv"
        p.write_text("table_name,column_name,data_type\n")
        df = ler_inventario(p)
        assert df.empty
        assert list(df.columns) == ["table_name", "column_name", "data_type"]


class TestLerEstrutura:
    def test_csv_valido(self, tmp_path: Path) -> None:
        p = tmp_path / "estrutura.csv"
        p.write_text(
            '"tabela","hash_estrutura_md5","linhas_estimadas","tamanho_total",'
            '"tamanho_tabela","tamanho_indices","tamanho_toast","qtd_colunas",'
            '"comentario","tuplas_vivas","tuplas_mortas","last_vacuum",'
            '"last_autovacuum","last_analyze","last_autoanalyze"\n'
            'tab1,"abc123","100","10 MB","10 MB","0 bytes","0 bytes","5",,'
            '"100","0",,,,\n'
        )
        df = ler_estrutura(p)
        assert "tabela" in df.columns
        assert "hash_estrutura_md5" in df.columns
        assert df["qtd_colunas"].iloc[0] == 5  # converted to numeric

    def test_colunas_obrigatorias(self, tmp_path: Path) -> None:
        # CSV mínimo
        p = tmp_path / "estrutura_min.csv"
        p.write_text(
            "tabela,hash_estrutura_md5,linhas_estimadas,qtd_colunas\ntab1,def456,50,3\n"
        )
        df = ler_estrutura(p)
        assert df["linhas_estimadas"].iloc[0] == 50
        assert df["qtd_colunas"].iloc[0] == 3


class TestLerComparacao:
    def test_csv_valido(self, tmp_path: Path) -> None:
        p = tmp_path / "comparacao.csv"
        p.write_text(
            '"hash_estrutura","quantidade_tabelas","tabelas"\n'
            '"abc123","2","sftp.tab1;dados_historicos.tab2"\n'
        )
        df = ler_comparacao(p)
        assert df["hash_estrutura"].iloc[0] == "abc123"
        assert df["quantidade_tabelas"].iloc[0] == 2

    def test_quantidade_convertida_para_numeric(self, tmp_path: Path) -> None:
        p = tmp_path / "comparacao_numeric.csv"
        p.write_text(
            '"hash_estrutura","quantidade_tabelas","tabelas"\n'
            '"xyz789","5","sftp.t1;dados_historicos.t2;sftp.t3"\n'
        )
        df = ler_comparacao(p)
        assert df["quantidade_tabelas"].dtype in ("int64", "float64")


class TestIndexarPorHash:
    def test_index_hash(self, tmp_path: Path) -> None:
        p = tmp_path / "comparacao.csv"
        p.write_text(
            '"hash_estrutura","quantidade_tabelas","tabelas"\n'
            '"abc123","2","sftp.tab1;dados_historicos.tab2"\n'
            '"def456","1","sftp.tab3"\n'
        )
        df = ler_comparacao(p)
        idx = indexar_por_hash(df)
        assert idx["abc123"] == {"sftp.tab1", "dados_historicos.tab2"}
        assert idx["def456"] == {"sftp.tab3"}

    def test_linha_sem_tabelas(self, tmp_path: Path) -> None:
        p = tmp_path / "comparacao_vazia.csv"
        p.write_text('"hash_estrutura","quantidade_tabelas","tabelas"\n"abc","0",""\n')
        df = ler_comparacao(p)
        idx = indexar_por_hash(df)
        assert "abc" not in idx

    def test_mesmo_hash_multiplas_linhas(self, tmp_path: Path) -> None:
        p = tmp_path / "comparacao_merge.csv"
        p.write_text(
            '"hash_estrutura","quantidade_tabelas","tabelas"\n'
            '"h1","1","sftp.a"\n'
            '"h1","1","dados_historicos.b"\n'
        )
        df = ler_comparacao(p)
        idx = indexar_por_hash(df)
        assert idx["h1"] == {"sftp.a", "dados_historicos.b"}
