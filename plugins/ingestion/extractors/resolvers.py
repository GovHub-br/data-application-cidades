"""Resolvedores de URL para `HttpRequest.resolve`: o link que muda a cada edição.

Cada função devolve o `resolve` de uma chamada: recebe o HttpHook GET da
Connection (com autenticação e cabeçalhos dela) e devolve a URL a baixar. A
descoberta falha alto: link que some da página é mudança de layout da fonte.
"""

from collections.abc import Callable, Mapping
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from ingestion.extractors.extractor_errors import ExtractionError

TIMEOUT_SECONDS = 60


def link_in_page(
    page: str, contains: str, headers: Mapping[str, str] | None = None
) -> Callable[[Any], str]:
    """O primeiro `<a href>` da página que contém `contains`, como URL completa."""

    def resolve(hook: Any) -> str:
        response = hook.run(
            page,
            headers=dict(headers or {}) or None,
            extra_options={"check_response": False, "timeout": TIMEOUT_SECONDS},
        )
        if response.status_code >= 400:
            raise ExtractionError(f"GET {page}: HTTP {response.status_code}")
        for href in _links(response.text):
            if contains in href:
                return urljoin(str(response.url), href)
        raise ExtractionError(f"nenhum link com {contains!r} em {page}")

    return resolve


def _links(document: str) -> list[str]:
    parser = _Links()
    parser.feed(document)
    return parser.hrefs


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.hrefs.append(href)
