"""Resolvedores de URL para `HttpRequest.resolve`: o link que muda a cada edição.

Cada função devolve o `resolve` de uma chamada: recebe o HttpHook GET da
Connection (com autenticação e cabeçalhos dela) e devolve a URL a baixar. A
descoberta falha alto: link que some da página é mudança de layout da fonte.
"""

from collections.abc import Callable, Mapping
from datetime import datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from ingestion.extractors.extractor_errors import ExtractionError
from ingestion.layout import TIMEZONE

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


def mziq_latest_file(
    company_id: str,
    category: str,
    headers: Mapping[str, str] | None = None,
    years_back: int = 3,
) -> Callable[[Any], str]:
    """Permalink do arquivo mais recente de uma categoria no catálogo da MZ (mziq).

    Sites de RI hospedados na MZ (a MRV, por exemplo) listam os documentos por ano
    numa API POST. Vale o ano mais recente que tiver documento da categoria e, nele,
    o maior trimestre (`file_quarter`). A busca volta `years_back` anos a partir
    do ano corrente em Brasília.
    """
    path = f"/filemanager/company/{company_id}/filter/categories/year/meta"

    def resolve(hook: Any) -> str:
        session = hook.get_conn(dict(headers or {}) or None)
        this_year = datetime.now(TIMEZONE).year
        for year in range(this_year, this_year - years_back, -1):
            response = session.post(
                hook.base_url + path,
                json={
                    "year": str(year),
                    "categories": [category],
                    "language": "pt_BR",
                    "published": True,
                },
                timeout=TIMEOUT_SECONDS,
            )
            if response.status_code >= 400:
                raise ExtractionError(f"POST {path}: HTTP {response.status_code}")
            documents = [
                document
                for document in response.json().get("data", {}).get("document_metas", [])
                if document.get("internal_name") == category and document.get("permalink")
            ]
            if documents:
                latest = max(documents, key=lambda d: d.get("file_quarter") or 0)
                return str(latest["permalink"])
        raise ExtractionError(
            f"nenhum documento {category!r} na MZ nos últimos {years_back} anos"
        )

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
