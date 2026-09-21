"""Testes do módulo de leitura de banco de dados (reader.py).

Cobre listar_tabelas, ler_tabela, ler_schema_colunas e calcular_hash_db.

Utiliza unittest.mock para simular o banco de dados PostgreSQL,
evitando a necessidade de um banco real. O módulo faz uso intensivo
de information_schema (PostgreSQL), que não é suportado pelo SQLite,
portanto todas as funções são testadas via mocking.
"""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from classificacao.db.reader import (
    ClassificationNotFoundError,
    calcular_hash_db,
    ler_classificacao_db,
    ler_schema_colunas,
    ler_tabela,
    listar_tabelas,
)


# ══════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture
def mock_engine() -> MagicMock:
    """Retorna um MagicMock que simula sqlalchemy.engine.Engine."""
    return MagicMock()


@pytest.fixture
def mock_connection(mock_engine: MagicMock) -> MagicMock:
    """Configura mock_engine.connect() para retornar uma conexão mockada
    que funciona como context manager."""
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_conn
    return mock_conn


# ══════════════════════════════════════════════════════════════════════════
# listar_tabelas
# ══════════════════════════════════════════════════════════════════════════


class TestListarTabelas:
    """Testes para listar_tabelas()."""

    def test_retorna_lista_ordenada(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Deve retornar lista ordenada alfabeticamente dos nomes de tabelas."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [
            ("tabela_b",),
            ("tabela_a",),
        ]
        mock_connection.execute.return_value = mock_result

        with patch(
            "classificacao.db.reader.get_schema_source", return_value="test_schema"
        ):
            tables = listar_tabelas(mock_engine, schema="test_schema")

        # A consulta SQL já faz ORDER BY, então retorna na ordem do DB
        assert tables == ["tabela_b", "tabela_a"]

        # Verifica que a query usa information_schema corretamente
        args, kwargs = mock_connection.execute.call_args
        assert "information_schema.tables" in str(args[0])
        assert "BASE TABLE" in str(args[0])
        assert args[1] == {"schema": "test_schema"}

    def test_schema_vazio_emite_warning(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Schema sem tabelas deve emitir UserWarning e retornar lista vazia."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_connection.execute.return_value = mock_result

        with (
            patch(
                "classificacao.db.reader.get_schema_source", return_value="empty_schema"
            ),
            pytest.warns(UserWarning, match="não contém tabelas"),
        ):
            tables = listar_tabelas(mock_engine, schema="empty_schema")

        assert tables == []

    def test_schema_none_usa_get_schema_source(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Quando schema=None, deve chamar get_schema_source() para obter o schema."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [("tab1",)]
        mock_connection.execute.return_value = mock_result

        with patch(
            "classificacao.db.reader.get_schema_source", return_value="default_schema"
        ) as mock_src:
            tables = listar_tabelas(mock_engine)  # schema=None

        mock_src.assert_called_once()
        args = mock_connection.execute.call_args
        assert args[0][1] == {"schema": "default_schema"}
        assert tables == ["tab1"]


# ══════════════════════════════════════════════════════════════════════════
# ler_tabela
# ══════════════════════════════════════════════════════════════════════════


class TestLerTabela:
    """Testes para ler_tabela()."""

    def test_retorna_dataframe_com_todas_colunas_string(
        self, mock_engine: MagicMock
    ) -> None:
        """Deve retornar um DataFrame com todas as colunas convertidas para str."""
        test_df = pd.DataFrame({"col_a": [1, 2], "col_b": [3.5, 4.5]})

        with (
            patch("classificacao.db.reader._tabela_existe", return_value=True),
            patch("classificacao.db.reader.pd.read_sql_table", return_value=test_df),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            df = ler_tabela(mock_engine, "tabela_x")

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["col_a", "col_b"]
        assert pd.api.types.is_string_dtype(df["col_a"])
        assert pd.api.types.is_string_dtype(df["col_b"])
        assert list(df["col_a"]) == ["1", "2"]
        assert list(df["col_b"]) == ["3.5", "4.5"]

    def test_drop_unnamed_0_sequencial(self, mock_engine: MagicMock) -> None:
        """Coluna unnamed_0 com valores sequenciais '0','1','2'... deve ser removida."""
        test_df = pd.DataFrame(
            {"unnamed_0": ["0", "1", "2", "3"], "col": ["a", "b", "c", "d"]}
        )

        with (
            patch("classificacao.db.reader._tabela_existe", return_value=True),
            patch("classificacao.db.reader.pd.read_sql_table", return_value=test_df),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            df = ler_tabela(mock_engine, "tabela_x")

        assert "unnamed_0" not in df.columns
        assert list(df["col"]) == ["a", "b", "c", "d"]

    def test_keep_unnamed_0_nao_sequencial(self, mock_engine: MagicMock) -> None:
        """Coluna unnamed_0 com valores NÃO sequenciais deve ser mantida."""
        test_df = pd.DataFrame(
            {"unnamed_0": ["0", "5", "2", "9"], "col": ["a", "b", "c", "d"]}
        )

        with (
            patch("classificacao.db.reader._tabela_existe", return_value=True),
            patch("classificacao.db.reader.pd.read_sql_table", return_value=test_df),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            df = ler_tabela(mock_engine, "tabela_x")

        assert "unnamed_0" in df.columns
        assert list(df["unnamed_0"]) == ["0", "5", "2", "9"]

    def test_keep_unnamed_0_poucas_linhas(self, mock_engine: MagicMock) -> None:
        """DataFrame com menos de 3 linhas não deve remover unnamed_0 (proteção)."""
        test_df = pd.DataFrame({"unnamed_0": ["0", "1"], "col": ["a", "b"]})

        with (
            patch("classificacao.db.reader._tabela_existe", return_value=True),
            patch("classificacao.db.reader.pd.read_sql_table", return_value=test_df),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            df = ler_tabela(mock_engine, "tabela_x")

        assert "unnamed_0" in df.columns

    def test_sem_coluna_unnamed_0(self, mock_engine: MagicMock) -> None:
        """DataFrame sem a coluna unnamed_0 não deve ser alterado."""
        test_df = pd.DataFrame({"col_a": ["x", "y"], "col_b": ["1", "2"]})

        with (
            patch("classificacao.db.reader._tabela_existe", return_value=True),
            patch("classificacao.db.reader.pd.read_sql_table", return_value=test_df),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            df = ler_tabela(mock_engine, "tabela_x")

        assert list(df.columns) == ["col_a", "col_b"]

    def test_tabela_inexistente(self, mock_engine: MagicMock) -> None:
        """Tabela inexistente deve levantar ValueError."""
        with (
            patch("classificacao.db.reader._tabela_existe", return_value=False),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
            pytest.raises(ValueError, match="não encontrada"),
        ):
            ler_tabela(mock_engine, "tabela_x")

    def test_schema_none_usa_get_schema_source(self, mock_engine: MagicMock) -> None:
        """Quando schema=None, deve obter schema via get_schema_source()."""
        test_df = pd.DataFrame({"col": ["val"]})

        with (
            patch(
                "classificacao.db.reader._tabela_existe", return_value=True
            ) as mock_existe,
            patch(
                "classificacao.db.reader.pd.read_sql_table", return_value=test_df
            ) as mock_read,
            patch(
                "classificacao.db.reader.get_schema_source",
                return_value="default_schema",
            ) as mock_src,
        ):
            df = ler_tabela(mock_engine, "tab1")

        mock_src.assert_called_once()
        # _tabela_existe deve ter sido chamado com o schema padrão
        assert mock_existe.call_args[0][2] == "default_schema"
        # read_sql_table deve ter sido chamado com schema="default_schema"
        assert mock_read.call_args[1]["schema"] == "default_schema"
        assert list(df["col"]) == ["val"]


# ══════════════════════════════════════════════════════════════════════════
# ler_schema_colunas
# ══════════════════════════════════════════════════════════════════════════


class TestLerSchemaColunas:
    """Testes para ler_schema_colunas()."""

    def test_retorna_dataframe_com_colunas_esperadas(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Deve retornar DataFrame com colunas table_name, column_name, data_type."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [
            ("tab1", "col_a", "integer"),
            ("tab1", "col_b", "text"),
            ("tab2", "col_x", "boolean"),
        ]
        mock_connection.execute.return_value = mock_result

        with patch(
            "classificacao.db.reader.get_schema_source", return_value="test_schema"
        ):
            df = ler_schema_colunas(mock_engine, schema="test_schema")

        assert list(df.columns) == ["table_name", "column_name", "data_type"]
        assert len(df) == 3
        assert df["table_name"].iloc[0] == "tab1"
        assert df["column_name"].iloc[1] == "col_b"
        assert df["data_type"].iloc[2] == "boolean"
        # Todas as colunas são string
        assert pd.api.types.is_string_dtype(df["table_name"])
        assert pd.api.types.is_string_dtype(df["column_name"])
        assert pd.api.types.is_string_dtype(df["data_type"])

    def test_schema_vazio_retorna_dataframe_vazio(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Schema sem colunas deve retornar DataFrame vazio."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_connection.execute.return_value = mock_result

        with patch(
            "classificacao.db.reader.get_schema_source", return_value="empty_schema"
        ):
            df = ler_schema_colunas(mock_engine, schema="empty_schema")

        # Quando não há linhas, o DataFrame é criado sem colunas definidas
        assert df.empty
        assert len(df) == 0

    def test_consulta_usa_information_schema(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Deve consultar information_schema.columns com o schema correto."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_connection.execute.return_value = mock_result

        with patch(
            "classificacao.db.reader.get_schema_source", return_value="test_schema"
        ):
            ler_schema_colunas(mock_engine, schema="test_schema")

        args, kwargs = mock_connection.execute.call_args
        assert "information_schema.columns" in str(args[0])
        assert "ordinal_position" in str(args[0])
        assert args[1] == {"schema": "test_schema"}

    def test_schema_none_usa_get_schema_source(
        self, mock_engine: MagicMock, mock_connection: MagicMock
    ) -> None:
        """Quando schema=None, deve chamar get_schema_source()."""
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_connection.execute.return_value = mock_result

        with patch(
            "classificacao.db.reader.get_schema_source", return_value="default_schema"
        ) as mock_src:
            ler_schema_colunas(mock_engine)  # schema=None

        mock_src.assert_called_once()
        args = mock_connection.execute.call_args
        assert args[0][1] == {"schema": "default_schema"}


# ══════════════════════════════════════════════════════════════════════════
# calcular_hash_db
# ══════════════════════════════════════════════════════════════════════════


class TestCalcularHashDb:
    """Testes para calcular_hash_db()."""

    def test_mesmo_conteudo_mesmo_hash(self, mock_engine: MagicMock) -> None:
        """Tabelas com conteúdo idêntico devem produzir hashes iguais."""
        test_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        with (
            patch(
                "classificacao.db.reader.pd.read_sql_table",
                side_effect=lambda *a, **kw: iter([test_df.copy()]),
            ),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            hash1 = calcular_hash_db(mock_engine, "tab1")
            hash2 = calcular_hash_db(mock_engine, "tab2")

        assert hash1 == hash2
        assert isinstance(hash1, str)
        assert len(hash1) == 32  # MD5 hex digest

    def test_conteudo_diferente_hash_diferente(self, mock_engine: MagicMock) -> None:
        """Tabelas com conteúdo diferente devem produzir hashes diferentes."""
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [1, 2], "b": [3, 5]})

        with (
            patch(
                "classificacao.db.reader.pd.read_sql_table",
                side_effect=[iter([df1]), iter([df2])],
            ),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            hash1 = calcular_hash_db(mock_engine, "tab1")
            hash2 = calcular_hash_db(mock_engine, "tab2")

        assert hash1 != hash2

    def test_hash_deterministico(self, mock_engine: MagicMock) -> None:
        """Chamadas repetidas para a mesma tabela devem produzir o mesmo hash."""
        test_df = pd.DataFrame({"x": ["foo", "bar"], "y": ["10", "20"]})

        with (
            patch(
                "classificacao.db.reader.pd.read_sql_table",
                side_effect=lambda *a, **kw: iter([test_df.copy()]),
            ),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            hash1 = calcular_hash_db(mock_engine, "tab1")
            hash2 = calcular_hash_db(mock_engine, "tab1")

        assert hash1 == hash2

    def test_chunksize_1000_utilizado(self, mock_engine: MagicMock) -> None:
        """Deve chamar read_sql_table com chunksize=1000."""
        test_df = pd.DataFrame({"a": [1]})

        with (
            patch(
                "classificacao.db.reader.pd.read_sql_table",
                return_value=iter([test_df]),
            ) as mock_read,
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            calcular_hash_db(mock_engine, "tab1")

        mock_read.assert_called_once()
        assert mock_read.call_args[1].get("chunksize") == 1000

    def test_hash_multiplos_chunks(self, mock_engine: MagicMock) -> None:
        """Múltiplos chunks devem ser processados corretamente (soma parcial)."""
        df1 = pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]})
        df2 = pd.DataFrame({"a": ["3", "4"], "b": ["z", "w"]})

        with (
            patch(
                "classificacao.db.reader.pd.read_sql_table",
                return_value=iter([df1, df2]),
            ),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            hash_result = calcular_hash_db(mock_engine, "tab1")

        # Hash esperado: 2 chunks processados em sequência
        hasher = hashlib.md5()
        for chunk in [df1, df2]:
            chunk_str = chunk.astype(str)
            chunk_str = chunk_str[sorted(chunk_str.columns)]
            hasher.update(str(chunk_str).encode("utf-8"))
        expected = hasher.hexdigest()

        assert hash_result == expected

    def test_tabela_vazia(self, mock_engine: MagicMock) -> None:
        """Tabela vazia (sem chunks) deve produzir hash de string vazia."""
        with (
            patch("classificacao.db.reader.pd.read_sql_table", return_value=iter([])),
            patch(
                "classificacao.db.reader.get_schema_source", return_value="test_schema"
            ),
        ):
            hash_result = calcular_hash_db(mock_engine, "empty_table")

        expected = hashlib.md5().hexdigest()
        assert hash_result == expected


# ══════════════════════════════════════════════════════════════════════════
# ler_classificacao_db
# ══════════════════════════════════════════════════════════════════════════


class TestLerClassificacaoDb:
    """Testes para ler_classificacao_db()."""

    def test_ler_classificacao_sucesso(self) -> None:
        """Tabela existe e tem dados: retorna DataFrame com colunas esperadas."""
        mock_engine = MagicMock()
        mock_df = pd.DataFrame(
            {
                "table_name": ["tab1", "tab2"],
                "formacao": ["bem_formada", "sem_cabecalho"],
                "confidence": ["high", "low"],
                "notes": ["", "colunas ausentes"],
                "extra_col": ["ignorar", "ignorar"],
            }
        )

        with (
            patch("classificacao.db.reader.pd.read_sql_table", return_value=mock_df),
            patch("classificacao.db.reader._tabela_existe", return_value=True),
        ):
            result = ler_classificacao_db(mock_engine, schema="target")

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "table_name",
                "formacao",
                "confidence",
                "notes",
            ]
            assert len(result) == 2
            assert result["table_name"].iloc[0] == "tab1"

    def test_ler_classificacao_tabela_nao_existe(self) -> None:
        """Tabela não existe: levanta ClassificationNotFoundError."""
        mock_engine = MagicMock()

        with patch("classificacao.db.reader._tabela_existe", return_value=False):
            with pytest.raises(ClassificationNotFoundError, match="não encontrada"):
                ler_classificacao_db(mock_engine, schema="target")

    def test_ler_classificacao_tabela_vazia(self) -> None:
        """Tabela existe mas está vazia: levanta ClassificationNotFoundError."""
        mock_engine = MagicMock()
        mock_df = pd.DataFrame()

        with (
            patch("classificacao.db.reader.pd.read_sql_table", return_value=mock_df),
            patch("classificacao.db.reader._tabela_existe", return_value=True),
        ):
            with pytest.raises(ClassificationNotFoundError, match="vazia"):
                ler_classificacao_db(mock_engine, schema="target")

    def test_ler_classificacao_colunas_faltando(self) -> None:
        """Tabela existe mas não tem colunas esperadas: levanta erro."""
        mock_engine = MagicMock()
        mock_df = pd.DataFrame(
            {
                "table_name": ["tab1"],
                "formacao": ["bem_formada"],
                # faltando confidence e notes
            }
        )

        with (
            patch("classificacao.db.reader.pd.read_sql_table", return_value=mock_df),
            patch("classificacao.db.reader._tabela_existe", return_value=True),
        ):
            with pytest.raises(ClassificationNotFoundError, match="colunas esperadas"):
                ler_classificacao_db(mock_engine, schema="target")
