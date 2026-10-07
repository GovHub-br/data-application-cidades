"""Testes do módulo de escrita em banco de dados (writer.py).

Cobre sanitize_table_name, criar_schema_target, escrever_tabela
e os wrappers escrever_classificacao, escrever_qualidade, escrever_dedup_map.

Utiliza unittest.mock para simular engine, conexões e chamadas to_sql,
evitando a necessidade de um banco PostgreSQL real.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from classificacao.db.writer import (
    comentar_tabela,
    criar_schema_target,
    escrever_classificacao,
    escrever_dedup_map,
    escrever_qualidade,
    escrever_tabela,
    sanitize_column_names,
    sanitize_table_name,
)


# ══════════════════════════════════════════════════════════════════════════
# sanitize_table_name
# ══════════════════════════════════════════════════════════════════════════


class TestSanitizeTableName:
    """Testes para sanitize_table_name()."""

    def test_replaces_special_chars(self) -> None:
        """Caracteres especiais devem ser substituídos por underscore."""
        assert sanitize_table_name("nome?com\\espacos") == "nome_com_espacos"

    def test_replaces_multiple_special_chars(self) -> None:
        """Múltiplos caracteres especiais consecutivos viram underscores."""
        assert sanitize_table_name("a!b@c#d") == "a_b_c_d"

    def test_truncates_to_63_chars(self) -> None:
        """Nomes com mais de 63 caracteres devem ser truncados."""
        long_name = "a" * 100
        result = sanitize_table_name(long_name)
        assert len(result) == 63
        assert result == "a" * 63

    def test_truncates_no_cutoff_word(self) -> None:
        """Truncamento deve ser simples (não quebra em palavras)."""
        name = "x" * 70
        result = sanitize_table_name(name)
        assert len(result) == 63
        assert result == "x" * 63

    def test_lowercase(self) -> None:
        """Nomes em maiúsculas devem ser convertidos para minúsculas."""
        assert sanitize_table_name("NOME_GRANDE") == "nome_grande"

    def test_mixed_case(self) -> None:
        """Nomes mistos devem ficar completamente em minúsculas."""
        assert sanitize_table_name("TabelaDeTeste") == "tabeladeteste"

    def test_starts_with_letter(self) -> None:
        """Nome iniciando com número deve receber underscore prefixado."""
        assert sanitize_table_name("123_nome") == "_123_nome"

    def test_starts_with_underscore_kept(self) -> None:
        """Nome já iniciando com underscore não é alterado nesse aspecto."""
        assert sanitize_table_name("_tabela") == "_tabela"

    def test_only_numbers(self) -> None:
        """Nome composto apenas de números deve receber underscore prefixado."""
        assert sanitize_table_name("12345") == "_12345"

    def test_empty_string(self) -> None:
        """String vazia deve resultar em '_table' (fallback seguro)."""
        assert sanitize_table_name("") == "_table"

    def test_all_underscore(self) -> None:
        """Nome já seguro não deve ser alterado (apenas lowercasing)."""
        assert sanitize_table_name("MINHA_TABELA") == "minha_tabela"

    def test_dots_and_hyphens(self) -> None:
        """Pontos e hífens devem ser substituídos."""
        assert sanitize_table_name("schema.table-name") == "schema_table_name"

    def test_leading_digit_after_truncation(self) -> None:
        """Nome com dígito na primeira posição trunca para 62 antes do prepend.

        Quando o nome original tem 63 caracteres válidos e começa com
        dígito, o truncamento reserva 1 caractere para o underscore
        prefixado, resultando em 63 caracteres no total.
        """
        name = "1" * 63
        result = sanitize_table_name(name)
        # Após substituições (nenhuma): 63 chars.
        # Truncamento (leading digit → 62 chars): 62 chars.
        # Lowercase: 62 chars (já está minúsculo).
        # Prepending: "_" + "1"*62 = 63 chars.
        assert result == "_" + "1" * 62
        assert len(result) == 63


# ══════════════════════════════════════════════════════════════════════════
# sanitize_column_names
# ══════════════════════════════════════════════════════════════════════════


class TestSanitizeColumnNames:
    """Testes para sanitize_column_names()."""

    def test_short_name_unchanged(self) -> None:
        """Nomes curtos (≤ max_len) não devem ser alterados."""
        df = pd.DataFrame({"a": [1], "bc": [2], "nome_curto": [3]})
        result = sanitize_column_names(df, max_len=63)
        assert list(result.columns) == ["a", "bc", "nome_curto"]

    def test_long_name_truncated_with_suffix(self) -> None:
        """Nomes longos (> max_len) devem ser truncados preservando o final.

        O resultado deve ter exatamente max_len caracteres, com prefixo
        '...' indicando o truncamento.
        """
        base = "abcdefghij"  # 10 chars
        # Repetir até formar um nome de 100 caracteres
        long_name = (base * 10)[:100]  # 100 chars
        df = pd.DataFrame({long_name: [1]})
        result = sanitize_column_names(df, max_len=63)
        col = result.columns[0]
        assert len(col) == 63
        # Deve terminar com os últimos 60 caracteres do nome original
        assert col == "..." + long_name[-(63 - 3) :]

    def test_collision_appends_suffix(self) -> None:
        """Duas colunas que truncam para o mesmo nome ganham sufixo _2."""
        # Dois nomes de 70 caracteres cujo truncamento preserva o final
        # — ambos viram "..." + últimos 60 = 63 chars
        name_a = "a" * 70
        name_b = "b" + "a" * 69  # difere no 1º char, mas últimos 60 são "a"*60
        df = pd.DataFrame({name_a: [1], name_b: [2]})
        result = sanitize_column_names(df, max_len=63)
        cols = list(result.columns)
        assert cols[0] == "..." + "a" * 60
        # Segundo: precisa de sufixo _2 → base de 61 + "_2" = 63
        assert cols[1] == "..." + "a" * 58 + "_2"

    def test_suffix_does_not_exceed_max_len(self) -> None:
        """Sufixo _N não deve fazer o nome ultrapassar max_len."""
        # Duas colunas de 62 chars idênticas → collisão
        name = "x" * 62
        df = pd.DataFrame(
            [(1, 2)],
            columns=[name, name],
        )
        result = sanitize_column_names(df, max_len=63)
        cols = list(result.columns)
        for c in cols:
            assert len(c) <= 63, f"Nome '{c}' tem {len(c)} > 63"
        # Primeira permanece igual (62 chars ≤ 63)
        assert cols[0] == "x" * 62
        # Segunda: base de 62 + "_2" = 64 → encurta para 61 + "_2" = 63
        assert cols[1] == "x" * 61 + "_2"

    def test_multiple_collisions_incremental_counter(self) -> None:
        """Três colunas com mesmo nome recebem sufixos _2 e _3."""
        df = pd.DataFrame(
            [(1, 2, 3)],
            columns=["coluna", "coluna", "coluna"],
        )
        result = sanitize_column_names(df, max_len=63)
        cols = list(result.columns)
        assert cols[0] == "coluna"
        assert cols[1] == "coluna_2"
        assert cols[2] == "coluna_3"


# ══════════════════════════════════════════════════════════════════════════
# criar_schema_target
# ══════════════════════════════════════════════════════════════════════════


class TestCriarSchemaTarget:
    """Testes para criar_schema_target()."""

    @staticmethod
    def _mock_execute_no_schema() -> MagicMock:
        """Retorna mock de execute cujo .scalar() indica schema NÃO existe."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = False
        return mock_result

    @staticmethod
    def _mock_execute_schema_exists() -> MagicMock:
        """Retorna mock de execute cujo .scalar() indica schema existe."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        return mock_result

    def test_schema_inexistente_cria_sem_drop(self) -> None:
        """Schema não existe + confirm=False: apenas CREATE, sem DROP."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_engine.connect.return_value = mock_conn
        mock_conn.execute.return_value = self._mock_execute_no_schema()

        with patch(
            "classificacao.db.writer.get_schema_target", return_value="test_schema"
        ):
            criar_schema_target(mock_engine, schema="test_schema", confirm=False)

        # EXISTS check + CREATE = 2 chamadas
        assert mock_conn.execute.call_count == 2
        calls = mock_conn.execute.call_args_list

        # 1ª: SELECT EXISTS
        exists_sql = str(calls[0][0][0])
        assert "information_schema.schemata" in exists_sql

        # 2ª: CREATE SCHEMA
        create_sql = str(calls[1][0][0])
        assert "CREATE SCHEMA" in create_sql
        assert "test_schema" in create_sql

        mock_conn.commit.assert_called_once()

    def test_schema_existente_sem_confirm_levanta_erro(self) -> None:
        """Schema existe + confirm=False: RuntimeError."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_engine.connect.return_value = mock_conn
        mock_conn.execute.return_value = self._mock_execute_schema_exists()

        with patch(
            "classificacao.db.writer.get_schema_target", return_value="test_schema"
        ):
            with pytest.raises(RuntimeError, match="Schema.*já existe"):
                criar_schema_target(mock_engine, schema="test_schema", confirm=False)

    def test_schema_existente_com_confirm_executa_drop_e_create(self) -> None:
        """Schema existe + confirm=True: DROP CASCADE + CREATE."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_engine.connect.return_value = mock_conn
        mock_conn.execute.return_value = self._mock_execute_schema_exists()

        with patch(
            "classificacao.db.writer.get_schema_target", return_value="test_schema"
        ):
            criar_schema_target(mock_engine, schema="test_schema", confirm=True)

        # EXISTS check + DROP + CREATE = 3 chamadas
        assert mock_conn.execute.call_count == 3
        calls = mock_conn.execute.call_args_list

        drop_sql = str(calls[1][0][0])
        assert "DROP SCHEMA" in drop_sql
        assert "test_schema" in drop_sql
        assert "CASCADE" in drop_sql

        create_sql = str(calls[2][0][0])
        assert "CREATE SCHEMA" in create_sql
        assert "test_schema" in create_sql

        mock_conn.commit.assert_called_once()

    def test_schema_none_usa_get_schema_target(self) -> None:
        """Quando schema=None, deve obter schema via get_schema_target()."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_engine.connect.return_value = mock_conn
        mock_conn.execute.return_value = self._mock_execute_no_schema()

        with patch(
            "classificacao.db.writer.get_schema_target", return_value="schema_padrao"
        ) as mock_src:
            criar_schema_target(mock_engine, confirm=True)

        mock_src.assert_called_once()
        # EXISTS check usa schema_padrao como parâmetro SQLAlchemy
        exists_call_args = mock_conn.execute.call_args_list[0].args
        assert exists_call_args[1] == {"schema": "schema_padrao"}
        # CREATE também referencia schema_padrao (interpolado no texto)
        create_sql = str(mock_conn.execute.call_args_list[1].args[0])
        assert "schema_padrao" in create_sql


# ══════════════════════════════════════════════════════════════════════════
# escrever_tabela
# ══════════════════════════════════════════════════════════════════════════


class TestEscreverTabela:
    """Testes para escrever_tabela()."""

    def test_calls_to_sql_with_sanitized_name(self) -> None:
        """Deve chamar df.to_sql com nome sanitizado, schema,
        if_exists='replace', index=False."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        with (
            patch(
                "classificacao.db.writer.get_schema_target",
                return_value="target_schema",
            ),
            patch.object(df, "to_sql") as mock_to_sql,
        ):
            escrever_tabela(df, "Test Table!", mock_engine)

        mock_to_sql.assert_called_once_with(
            name="test_table_",
            con=mock_engine,
            schema="target_schema",
            if_exists="replace",
            index=False,
        )

    def test_passes_schema_explicitly(self) -> None:
        """Quando schema é fornecido explicitamente, não chama get_schema_target."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"x": ["1"]})

        with (
            patch("classificacao.db.writer.get_schema_target") as mock_src,
            patch.object(df, "to_sql") as mock_to_sql,
        ):
            escrever_tabela(df, "tab", mock_engine, schema="custom_schema")

        mock_src.assert_not_called()
        mock_to_sql.assert_called_once()
        assert mock_to_sql.call_args[1]["schema"] == "custom_schema"

    def test_schema_none_usa_get_schema_target(self) -> None:
        """Quando schema=None, deve obter schema via get_schema_target()."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"x": ["1"]})

        with (
            patch(
                "classificacao.db.writer.get_schema_target",
                return_value="default_schema",
            ),
            patch.object(df, "to_sql") as mock_to_sql,
        ):
            escrever_tabela(df, "tab", mock_engine)  # schema=None

        assert mock_to_sql.call_args[1]["schema"] == "default_schema"


# ══════════════════════════════════════════════════════════════════════════
# Wrappers: escrever_classificacao, escrever_qualidade, escrever_dedup_map
# ══════════════════════════════════════════════════════════════════════════


class TestWrappers:
    """Testes para os wrappers de conveniência."""

    def test_escrever_classificacao_chama_escrever_tabela(
        self,
    ) -> None:
        """escrever_classificacao deve chamar escrever_tabela com '_classificacao'."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"table_name": ["t1"], "formacao": ["bem_formada"]})

        with patch("classificacao.db.writer.escrever_tabela") as mock_escrever:
            escrever_classificacao(df, mock_engine, schema="target")

        mock_escrever.assert_called_once_with(
            df, "_classificacao", mock_engine, schema="target"
        )

    def test_escrever_classificacao_schema_default(
        self,
    ) -> None:
        """Quando schema não é informado, deve passar None para escrever_tabela."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"table_name": ["t1"]})

        with patch("classificacao.db.writer.escrever_tabela") as mock_escrever:
            escrever_classificacao(df, mock_engine)

        # schema=None, então escrever_tabela usará get_schema_target()
        mock_escrever.assert_called_once_with(
            df, "_classificacao", mock_engine, schema=None
        )

    def test_escrever_qualidade_chama_escrever_tabela(
        self,
    ) -> None:
        """escrever_qualidade deve chamar escrever_tabela com '_qualidade'."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"table_name": ["t1"], "missing_pct": [0.05]})

        with patch("classificacao.db.writer.escrever_tabela") as mock_escrever:
            escrever_qualidade(df, mock_engine, schema="target")

        mock_escrever.assert_called_once_with(
            df, "_qualidade", mock_engine, schema="target"
        )

    def test_escrever_qualidade_schema_default(
        self,
    ) -> None:
        """Quando schema não é informado, deve passar None para escrever_tabela."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"table_name": ["t1"]})

        with patch("classificacao.db.writer.escrever_tabela") as mock_escrever:
            escrever_qualidade(df, mock_engine)

        mock_escrever.assert_called_once_with(
            df, "_qualidade", mock_engine, schema=None
        )

    def test_escrever_dedup_map_chama_escrever_tabela(
        self,
    ) -> None:
        """escrever_dedup_map deve chamar escrever_tabela com '_dedup_map'."""
        mock_engine = MagicMock()
        df = pd.DataFrame(
            {"source_table": ["t1"], "content_hash": ["abc"], "canonical_table": ["t1"]}
        )

        with patch("classificacao.db.writer.escrever_tabela") as mock_escrever:
            escrever_dedup_map(df, mock_engine, schema="target")

        mock_escrever.assert_called_once_with(
            df, "_dedup_map", mock_engine, schema="target"
        )

    def test_escrever_dedup_map_schema_default(
        self,
    ) -> None:
        """Quando schema não é informado, deve passar None para escrever_tabela."""
        mock_engine = MagicMock()
        df = pd.DataFrame({"source_table": ["t1"]})

        with patch("classificacao.db.writer.escrever_tabela") as mock_escrever:
            escrever_dedup_map(df, mock_engine)

        mock_escrever.assert_called_once_with(
            df, "_dedup_map", mock_engine, schema=None
        )


# ── comentar_tabela ──────────────────────────────────────────────────────


class TestComentarTabela:
    """Testes para a função comentar_tabela."""

    def test_comentario_executado_com_formato_correto(
        self,
    ) -> None:
        """COMMENT ON TABLE é executado com a string key=val;..."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        comentar_tabela(
            mock_engine,
            "schema",
            "table",
            {"source": "sftp.tabela", "formacao": "bem_formada", "rows": "99"},
        )

        mock_conn.execute.assert_called_once()
        # Check the SQL contains expected parts
        call_args = mock_conn.execute.call_args[0][0]
        sql_text = str(call_args)
        assert "COMMENT ON TABLE" in sql_text
        assert '"schema"."table"' in sql_text

    def test_sanitizacao_caracteres_especiais(
        self,
    ) -> None:
        """Valores com ; e = são sanitizados."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        comentar_tabela(
            mock_engine,
            "s",
            "t",
            {"key": "val;ue", "key2": "a=b"},
        )

        call_args = mock_conn.execute.call_args[0][1]
        comment = call_args["comment"]
        assert ";" not in comment.split("=")[-1]  # no ; inside values
        assert "val,ue" in comment
        assert "a:b" in comment

    def test_truncamento_500_caracteres(
        self,
    ) -> None:
        """String serializada é truncada em 500 caracteres."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        comentar_tabela(
            mock_engine,
            "s",
            "t",
            {f"k{i}": "x" * 80 for i in range(20)},
        )

        call_args = mock_conn.execute.call_args[0][1]
        comment = call_args["comment"]
        assert len(comment) <= 500
        assert comment.endswith("...")

    def test_erro_no_comment_nao_lanca_excecao(
        self,
    ) -> None:
        """Falha no COMMENT ON TABLE não interrompe o pipeline."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = RuntimeError("permission denied")
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Should not raise
        comentar_tabela(mock_engine, "s", "t", {"key": "val"})

        # Verify execute was attempted
        mock_conn.execute.assert_called_once()
