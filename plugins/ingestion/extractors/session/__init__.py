"""Fluxo HTTP numa mesma sessão (login, formulário, download), como um navegador.

A estratégia `http_session` executa, em ordem, os passos declarados na DAG
(`HttpSession.steps`): requisições que capturam valores da resposta (regex, JSON,
cookie, cabeçalho) e conferem o que chegou, mudanças nos cabeçalhos da sessão e o
download do arquivo. Valores capturados e Variables entram nos passos seguintes
por `{nome}`. A interface segue a do `requests`.
"""

from ingestion.extractors.session.captures import (
    Contains,
    Cookie,
    Header,
    JsonEquals,
    JsonField,
    Regex,
)
from ingestion.extractors.session.config import HttpSession
from ingestion.extractors.session.steps import (
    Download,
    DropHeaders,
    Request,
    SetHeaders,
)

__all__ = [
    "Contains",
    "Cookie",
    "Download",
    "DropHeaders",
    "Header",
    "HttpSession",
    "JsonEquals",
    "JsonField",
    "Regex",
    "Request",
    "SetHeaders",
]
