"""O que as estratégias HTTP (`api`, `http_file`) compartilham, sobre o HttpHook.

Conexão, autenticação e base URL vêm da Connection; aqui fica a política de retry,
a tradução de status em erro de extração e a gravação do corpo em stream.
"""

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Connection
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.extractors.base_extractor import RawFile, write_stream
from ingestion.extractors.config_extractor import ExtractorConfig, HttpRequest
from ingestion.extractors.extractor_errors import ExtractionError, SourceNotFoundError

RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 2
TIMEOUT_SECONDS = 120
CHUNK_BYTES = 1024 * 1024


class _Retryable(Exception):
    """Resposta da qual a fonte pode se recuperar: vale tentar de novo."""


class _ServerError(_Retryable):
    """5xx."""

    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")


class _NotJson(_Retryable):
    """2xx com corpo que não é JSON numa API JSON: página de erro servida como
    sucesso (o SGS do BACEN faz isso de forma intermitente).
    """

    def __init__(self, content_type: str) -> None:
        super().__init__(f"resposta não é JSON ({content_type})")


def _declares_non_json(response: requests.Response) -> str | None:
    """O Content-Type, se a resposta declarar um que não é JSON; senão None."""
    content_type = response.headers.get("Content-Type", "")
    if content_type and "json" not in content_type.lower():
        return content_type
    return None


PUBLIC_CONN_ID = "http_publica"


class HttpHooks:
    """Um HttpHook por método HTTP, todos na mesma Connection (o hook fixa o método).

    Com `base_url` (fonte pública), a Connection é montada em memória a partir da
    URL, sem consulta ao Airflow.
    """

    def __init__(
        self, conn_id: str, adapter: Any = None, base_url: str | None = None
    ) -> None:
        self.conn_id = conn_id or PUBLIC_CONN_ID
        self.adapter = adapter
        self.base_url = base_url
        self._hooks: dict[str, HttpHook] = {}

    @classmethod
    def from_config(cls, config: ExtractorConfig) -> "HttpHooks":
        return cls(config.conn_id, config.adapter, config.base_url)

    def for_method(self, method: str) -> HttpHook:
        method = method.upper()
        if method not in self._hooks:
            hook = HttpHook(
                method=method, http_conn_id=self.conn_id, adapter=self.adapter
            )
            if self.base_url:
                public = Connection(
                    conn_id=self.conn_id, conn_type="http", host=self.base_url
                )
                hook.get_connection = lambda _conn_id: public  # type: ignore[method-assign,assignment]
            self._hooks[method] = hook
        return self._hooks[method]


def require_source(config: ExtractorConfig) -> None:
    """Estratégia HTTP sem Connection nem URL base não tem para onde ir."""
    if not config.conn_id and not config.base_url:
        raise ValueError(f"a estratégia {config.source} precisa de conn_id ou base_url")


def fetch(
    hooks: HttpHooks, request: HttpRequest, *, expect_json: bool = False
) -> requests.Response:
    """Abre a resposta em stream, com retry em 5xx e falha de rede.

    404 vira SourceNotFoundError; outro 4xx, ExtractionError sem retry (repetir não
    muda a resposta); 5xx que persiste, ExtractionError com o último status. Com
    `expect_json`, um 2xx que declara Content-Type não JSON também é repetido e, se
    persistir, vira ExtractionError: a raw nunca guarda uma página de erro como dado.
    """
    hook = hooks.for_method(request.method)
    endpoint = (
        request.resolve(hooks.for_method("GET")) if request.resolve else request.endpoint
    )
    kwargs: dict[str, Any] = {}
    if request.json is not None:
        kwargs["json"] = request.json
    params = dict(request.params) or None
    if hook.method != "GET" and params:
        kwargs["params"] = params  # fora do GET, o hook mandaria `data` no corpo
        params = None

    def attempt() -> requests.Response:
        response: requests.Response
        if urlparse(endpoint).scheme:
            # URL completa: o hook colaria a base na frente. Usa a sessão que ele
            # configurou (autenticação, cabeçalhos, adapter), com as mesmas regras.
            session = hook.get_conn(dict(request.headers) or None)
            response = session.request(
                hook.method,
                endpoint,
                params=params or kwargs.get("params"),
                json=kwargs.get("json"),
                stream=True,
                timeout=TIMEOUT_SECONDS,
            )
        else:
            response = hook.run(
                endpoint,
                data=params,
                headers=dict(request.headers) or None,
                extra_options={
                    "stream": True,
                    "check_response": False,
                    "timeout": TIMEOUT_SECONDS,
                },
                **kwargs,
            )
        _raise_if_retryable(response, expect_json)
        return response

    retrying = Retrying(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_WAIT_SECONDS, max=30),
        retry=retry_if_exception_type((_Retryable, requests.ConnectionError)),
        reraise=True,
    )
    where = f"{request.method} {endpoint}"
    try:
        response = retrying(attempt)
    except _Retryable as exc:
        raise ExtractionError(f"{where}: {exc} após {RETRY_ATTEMPTS} tentativas") from exc
    except requests.RequestException as exc:
        raise ExtractionError(f"{where}: {exc}") from exc

    if response.status_code == 404:
        response.close()
        raise SourceNotFoundError(f"{where}: HTTP 404")
    if response.status_code >= 400:
        response.close()
        raise ExtractionError(f"{where}: HTTP {response.status_code}")
    return response


def _raise_if_retryable(response: requests.Response, expect_json: bool) -> None:
    if response.status_code >= 500:
        response.close()
        raise _ServerError(response.status_code)
    if expect_json and response.status_code < 300:
        content_type = _declares_non_json(response)
        if content_type:
            response.close()
            raise _NotJson(content_type)


def save(response: requests.Response, path: Path, name: str) -> RawFile:
    """Grava o corpo como veio (sem decodificar), um bloco por vez."""
    try:
        return write_stream(response.iter_content(CHUNK_BYTES), path, name)
    finally:
        response.close()
