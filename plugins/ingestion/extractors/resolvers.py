"""Resolvedores de URL para `HttpRequest.resolve`: o link que muda a cada edição.

Cada função devolve o `resolve` de uma chamada: recebe o HttpHook GET da
Connection (com autenticação e cabeçalhos dela) e devolve a URL a baixar. A
descoberta falha alto: link que some da página é mudança de layout da fonte.
"""

from collections.abc import Callable, Mapping, Sequence
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


def latest_in_json_listing(
    endpoint: str,
    *,
    method: str = "GET",
    params: Mapping[str, Any] | None = None,
    json: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    items: str,
    where: Mapping[str, Any] | None = None,
    order_by: str | None = None,
    pick: str,
    attempts: Sequence[Mapping[str, Any]] = ({},),
) -> Callable[[Any], str]:
    """A URL do item mais recente numa listagem JSON (catálogo, API de documentos).

    Chama `endpoint` (relativo à Connection ou URL completa) e, na resposta:

    - `items`: caminho por pontos até a lista (`"data.document_metas"`);
    - `where`: só os itens com esses valores de campo;
    - `order_by`: o item com o maior valor nesse campo (sem ele, o primeiro);
    - `pick`: o campo com a URL.

    `attempts` são valores tentados em ordem até achar um item (os anos de um
    catálogo, por exemplo); cada tentativa entra no `json`, quando há corpo, ou nos
    `params`. Sem item em nenhuma tentativa, a descoberta falha.
    """

    def resolve(hook: Any) -> str:
        session = hook.get_conn(dict(headers or {}) or None)
        url = endpoint if "://" in endpoint else hook.base_url + endpoint
        for attempt in attempts:
            body = {**json, **attempt} if json is not None else None
            query = {**(params or {}), **attempt} if json is None else dict(params or {})
            response = session.request(
                method.upper(),
                url,
                params=query or None,
                json=body,
                timeout=TIMEOUT_SECONDS,
            )
            if response.status_code >= 400:
                raise ExtractionError(f"{method} {endpoint}: HTTP {response.status_code}")
            found = [
                item
                for item in _path(response.json(), items) or []
                if isinstance(item, dict)
                and all(item.get(k) == v for k, v in (where or {}).items())
                and item.get(pick)
            ]
            if found:
                chosen = (
                    max(found, key=lambda item: item.get(order_by) or 0)
                    if order_by
                    else found[0]
                )
                return str(chosen[pick])
        raise ExtractionError(
            f"nenhum item em {items!r} de {endpoint} com {dict(where or {})} "
            f"em {len(attempts)} tentativa(s)"
        )

    return resolve


def _path(document: Any, path: str) -> Any:
    for key in path.split("."):
        document = document.get(key) if isinstance(document, dict) else None
    return document


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
