"""Testes das funções helper de classificação em main.py."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# Adiciona raiz do repo ao path para importar main.py
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


class TestCarregarClassificacaoCsv:
    """Task 7.1: Testes para _carregar_classificacao_csv()."""

    def test_arquivo_existe_valido(self) -> None:
        """Arquivo existe e é válido: carrega DataFrame com colunas esperadas."""
        import main

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test_class.csv"
            df_orig = pd.DataFrame(
                {
                    "table_name": ["tab1", "tab2"],
                    "formacao": ["bem_formada", "sem_cabecalho"],
                    "confidence": ["high", "low"],
                    "notes": ["", "sem cabeçalho"],
                }
            )
            df_orig.to_csv(csv_path, index=False)

            result = main._carregar_classificacao_csv(str(csv_path))

            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == [
                "table_name",
                "formacao",
                "confidence",
                "notes",
            ]
            assert len(result) == 2

    def test_arquivo_nao_existe(self) -> None:
        """Arquivo não existe: sys.exit(1)."""
        import main

        with pytest.raises(SystemExit) as exc_info:
            main._carregar_classificacao_csv("/tmp/arquivo_inexistente_xyz.csv")

        assert exc_info.value.code == 1

    def test_arquivo_colunas_erradas(self) -> None:
        """Arquivo existe mas não tem colunas obrigatórias: sys.exit(1)."""
        import main

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "bad_class.csv"
            df_bad = pd.DataFrame(
                {
                    "coluna_x": ["a"],
                    "coluna_y": ["b"],
                }
            )
            df_bad.to_csv(csv_path, index=False)

            with pytest.raises(SystemExit) as exc_info:
                main._carregar_classificacao_csv(str(csv_path))

            assert exc_info.value.code == 1


class TestCarregarClassificacaoDb:
    """Task 7.3: Testes para _carregar_classificacao_db() com fallback."""

    def test_pg_disponivel_carrega_do_postgres(self) -> None:
        """Tabela PG existe: carrega do PostgreSQL sem tentar CSV."""
        import main
        from unittest.mock import MagicMock, patch

        mock_engine = MagicMock()
        mock_df = pd.DataFrame(
            {
                "table_name": ["tab1"],
                "formacao": ["bem_formada"],
                "confidence": ["high"],
                "notes": [""],
            }
        )

        with patch(
            "classificacao.db.reader.ler_classificacao_db", return_value=mock_df
        ):
            result = main._carregar_classificacao_db(mock_engine)

            assert len(result) == 1
            assert result["table_name"].iloc[0] == "tab1"

    def test_pg_falha_csv_existe_carrega_do_csv(self) -> None:
        """PG falha (ClassificationNotFoundError), CSV local existe: carrega do CSV."""
        import main
        from unittest.mock import MagicMock, patch
        from classificacao.db.reader import ClassificationNotFoundError

        mock_engine = MagicMock()
        mock_df = pd.DataFrame(
            {
                "table_name": ["tab_csv"],
                "formacao": ["sub_tabelas_1"],
                "confidence": ["low"],
                "notes": ["do csv"],
            }
        )

        with (
            patch(
                "classificacao.db.reader.ler_classificacao_db",
                side_effect=ClassificationNotFoundError("not found"),
            ),
            patch.object(Path, "exists", return_value=True),
            patch("main.pd.read_csv", return_value=mock_df),
        ):
            result = main._carregar_classificacao_db(mock_engine)

            assert len(result) == 1
            assert result["table_name"].iloc[0] == "tab_csv"

    def test_ambos_falham_sys_exit(self) -> None:
        """Nem PG nem CSV disponíveis: sys.exit(1)."""
        import main
        from unittest.mock import MagicMock, patch
        from classificacao.db.reader import ClassificationNotFoundError

        mock_engine = MagicMock()

        with (
            patch(
                "classificacao.db.reader.ler_classificacao_db",
                side_effect=ClassificationNotFoundError("not found"),
            ),
            patch.object(Path, "exists", return_value=False),
        ):
            with pytest.raises(SystemExit) as exc_info:
                main._carregar_classificacao_db(mock_engine)

            assert exc_info.value.code == 1
