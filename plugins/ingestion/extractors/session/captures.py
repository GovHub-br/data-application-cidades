"""Capturas (valores tirados da resposta) e conferências de um passo."""

import html
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any

import requests


def json_path(document: Any, path: str) -> Any:
    """Caminho por pontos; segmento numérico indexa lista. Ausente dá None."""
    for key in path.split("."):
        if isinstance(document, dict):
            document = document.get(key)
        elif isinstance(document, list) and key.isdigit() and int(key) < len(document):
            document = document[int(key)]
        else:
            return None
    return document


def _json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None


def _decode(value: str, unescape_html: bool, unquote_url: bool) -> str:
    if unquote_url:
        value = urllib.parse.unquote(value)
    if unescape_html:
        value = html.unescape(value)
    return value


@dataclass(frozen=True)
class Regex:
    """Grupo de uma expressão regular no corpo da resposta (texto).

    `unescape_html` desfaz entidades (`&amp;`); `unquote_url` desfaz `%XX`.
    """

    pattern: str
    group: int = 1
    unescape_html: bool = False
    unquote_url: bool = False
    default: str | None = None
    required: bool = True

    def extract(self, response: requests.Response, session: requests.Session) -> Any:
        match = re.search(self.pattern, response.text, re.DOTALL)
        if not match:
            return None
        return _decode(match.group(self.group), self.unescape_html, self.unquote_url)


@dataclass(frozen=True)
class JsonField:
    """Campo do corpo JSON, por caminho com pontos (`data.URL_Gratuito`)."""

    path: str
    default: str | None = None
    required: bool = True

    def extract(self, response: requests.Response, session: requests.Session) -> Any:
        value = json_path(_json(response), self.path)
        return None if value is None else str(value)


@dataclass(frozen=True)
class Cookie:
    """Cookie da sessão; `pattern` tira um grupo do valor (depois de `unquote_url`)."""

    name: str
    pattern: str | None = None
    unquote_url: bool = False
    default: str | None = None
    required: bool = True

    def extract(self, response: requests.Response, session: requests.Session) -> Any:
        value = session.cookies.get(self.name)
        if value is None:
            return None
        value = _decode(value, False, self.unquote_url)
        if self.pattern is None:
            return value
        match = re.search(self.pattern, value)
        return match.group(1) if match else None


@dataclass(frozen=True)
class Header:
    """Cabeçalho da resposta."""

    name: str
    default: str | None = None
    required: bool = True

    def extract(self, response: requests.Response, session: requests.Session) -> Any:
        return response.headers.get(self.name)


@dataclass(frozen=True)
class Contains:
    """O corpo da resposta tem de conter o texto."""

    text: str

    def holds(self, response: requests.Response) -> bool:
        return self.text in response.text

    def __str__(self) -> str:
        return f"a resposta não contém {self.text!r}"


@dataclass(frozen=True)
class JsonEquals:
    """O campo do corpo JSON tem de ter esse valor."""

    path: str
    value: Any

    def holds(self, response: requests.Response) -> bool:
        return bool(json_path(_json(response), self.path) == self.value)

    def __str__(self) -> str:
        return f"{self.path} diferente de {self.value!r}"
