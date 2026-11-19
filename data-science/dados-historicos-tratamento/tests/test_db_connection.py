"""Testes do módulo de conexão com banco de dados (connection.py).

Cobre get_engine, get_session, get_schema_source, get_schema_target
e a validação de variáveis de ambiente obrigatórias.

Utiliza monkeypatch do pytest e unittest.mock.patch para isolar
as funções do ambiente real e do PostgreSQL.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from classificacao.db.connection import (
    get_engine,
    get_session,
    get_schema_source,
    get_schema_target,
    get_schema_sftp_target,
)


# ─── get_schema_source ────────────────────────────────────────────────────


def test_get_schema_source_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_SCHEMA_SOURCE não está definida, deve retornar 'dados_historicos'."""
    monkeypatch.delenv("DB_SCHEMA_SOURCE", raising=False)
    assert get_schema_source() == "dados_historicos"


def test_get_schema_source_custom(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_SCHEMA_SOURCE está definida, deve retornar seu valor."""
    monkeypatch.setenv("DB_SCHEMA_SOURCE", "custom_schema")
    assert get_schema_source() == "custom_schema"


# ─── get_schema_target ────────────────────────────────────────────────────


def test_get_schema_target_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_SCHEMA_TARGET não está definida, deve retornar
    'dados_historicos_formatados'."""
    monkeypatch.delenv("DB_SCHEMA_TARGET", raising=False)
    assert get_schema_target() == "dados_historicos_formatados"


def test_get_schema_target_custom(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_SCHEMA_TARGET está definida, deve retornar seu valor."""
    monkeypatch.setenv("DB_SCHEMA_TARGET", "custom_target")
    assert get_schema_target() == "custom_target"


# ─── get_schema_sftp_target ───────────────────────────────────────────────


def test_get_schema_sftp_target_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_SCHEMA_SFTP_TARGET não está definida, deve retornar
    'sftp_tratado'."""
    monkeypatch.delenv("DB_SCHEMA_SFTP_TARGET", raising=False)
    assert get_schema_sftp_target() == "sftp_tratado"


def test_get_schema_sftp_target_custom(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_SCHEMA_SFTP_TARGET está definida, deve retornar seu valor."""
    monkeypatch.setenv("DB_SCHEMA_SFTP_TARGET", "sftp_producao")
    assert get_schema_sftp_target() == "sftp_producao"


# ─── get_engine ────────────────────────────────────────────────────────────


def test_get_engine_missing_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_HOST está ausente, levanta ValueError."""
    # Define todas as variáveis exceto DB_HOST
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "pass")
    monkeypatch.delenv("DB_HOST", raising=False)

    with pytest.raises(ValueError, match="DB_HOST"):
        get_engine()


def test_get_engine_missing_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_PASSWORD está ausente, levanta ValueError."""
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="DB_PASSWORD"):
        get_engine()


def test_get_engine_empty_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando DB_USER é string vazia, levanta ValueError."""
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_USER", "")
    monkeypatch.setenv("DB_PASSWORD", "pass")

    with pytest.raises(ValueError, match="DB_USER"):
        get_engine()


def test_get_engine_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Com todas as variáveis definidas, cria engine com a URL correta
    e os parâmetros pool_size=5, max_overflow=10, connect_timeout=30."""
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "pass")

    with patch("classificacao.db.connection.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        engine = get_engine()

        mock_create.assert_called_once()
        args, kwargs = mock_create.call_args
        url = args[0]

        # Verifica o formato da connection string
        assert "postgresql+psycopg2://user:pass@localhost:5432/testdb" in url
        assert kwargs["pool_size"] == 5
        assert kwargs["max_overflow"] == 10
        assert kwargs["connect_args"]["connect_timeout"] == 30
        assert engine is mock_engine


def test_get_engine_password_url_encoded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Senha com caracteres especiais é URL-encoded antes de montar a string."""
    monkeypatch.setenv("DB_HOST", "db.example.com")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "mydb")
    monkeypatch.setenv("DB_USER", "admin")
    monkeypatch.setenv("DB_PASSWORD", "p@ss:w?rd")

    with patch("classificacao.db.connection.create_engine") as mock_create:
        mock_create.return_value = MagicMock()
        get_engine()
        args = mock_create.call_args[0][0]
        # @ → %40, : → %3A, ? → %3F
        assert "p%40ss%3Aw%3Frd" in args


# ─── get_session ───────────────────────────────────────────────────────────


def test_get_session_creates_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Quando engine não é fornecido, get_session deve chamar get_engine()."""
    mock_engine = MagicMock()

    with patch(
        "classificacao.db.connection.get_engine", return_value=mock_engine
    ) as mock_get_eng:
        with patch("classificacao.db.connection.Session") as mock_session_cls:
            mock_session = MagicMock(spec=Session)
            mock_session_cls.return_value = mock_session

            session = get_session()

            mock_get_eng.assert_called_once()
            mock_session_cls.assert_called_once_with(mock_engine)
            assert session is mock_session


def test_get_session_with_explicit_engine() -> None:
    """Quando engine é fornecido, get_session não chama get_engine()."""
    mock_engine = MagicMock()

    with patch("classificacao.db.connection.get_engine") as mock_get_eng:
        with patch("classificacao.db.connection.Session") as mock_session_cls:
            mock_session = MagicMock(spec=Session)
            mock_session_cls.return_value = mock_session

            session = get_session(engine=mock_engine)

            mock_get_eng.assert_not_called()
            mock_session_cls.assert_called_once_with(mock_engine)
            assert session is mock_session
