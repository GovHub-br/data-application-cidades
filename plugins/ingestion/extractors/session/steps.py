"""Passos de um fluxo `http_session` (Command): cada um age sobre a sessão."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import requests

from ingestion.extractors.base_extractor import RawFile, write_stream
from ingestion.extractors.extractor_errors import ExtractionError
from ingestion.extractors.session.templates import render

TIMEOUT_SECONDS = 120
CHUNK_BYTES = 1024 * 1024


class Capture(Protocol):
    @property
    def default(self) -> str | None:
        """Valor quando a captura não acha nada (None: sem padrão)."""

    @property
    def required(self) -> bool:
        """Se a falta do valor (sem padrão) falha o passo."""

    def extract(self, response: requests.Response, session: requests.Session) -> Any:
        """O valor capturado, ou None se não achou."""


class Check(Protocol):
    def holds(self, response: requests.Response) -> bool:
        """Se a resposta passa na conferência."""


@dataclass
class Run:
    """Estado de uma execução do fluxo: a sessão, os valores e a pasta de trabalho."""

    session: requests.Session
    context: dict[str, str]
    work_dir: Path


@dataclass(frozen=True)
class Request:
    """Uma requisição, como `requests.request`, com capturas e conferências.

    - `data` é formulário e `json` é corpo JSON; textos aceitam `{nome}`;
    - `drop_empty`: campos do formulário que saem quando o valor fica vazio (o
      `__EVENTVALIDATION` que o ASP.NET às vezes não manda, por exemplo);
    - `capture`: nome → captura; sem valor, vale o `default`, ou o valor anterior
      se `required=False`, ou o passo falha;
    - `expect`: conferências da resposta;
    - `check_status=False`: aceita qualquer status (passo de melhor esforço, que
      só alimenta `default`s).
    """

    name: str
    method: str
    url: str
    params: Mapping[str, Any] | None = None
    data: Mapping[str, Any] | None = None
    json: Any = None
    headers: Mapping[str, str] | None = None
    capture: Mapping[str, Capture] = field(default_factory=dict)
    expect: tuple[Check, ...] = ()
    drop_empty: tuple[str, ...] = ()
    check_status: bool = True

    def execute(self, run: Run) -> RawFile | None:
        response = self._send(run, stream=False)
        try:
            self._verify(response, run)
        finally:
            response.close()
        return None

    def _send(self, run: Run, stream: bool) -> requests.Response:
        data = render(self.data, run.context)
        if data is not None:
            data = {
                key: value
                for key, value in data.items()
                if not (key in self.drop_empty and value in ("", None))
            }
        try:
            response = run.session.request(
                self.method.upper(),
                render(self.url, run.context),
                params=render(self.params, run.context),
                data=data,
                json=render(self.json, run.context),
                headers=render(self.headers, run.context),
                timeout=TIMEOUT_SECONDS,
                stream=stream,
            )
        except requests.RequestException as exc:
            raise ExtractionError(f"{self.name}: {exc}") from exc
        if self.check_status and response.status_code >= 400:
            response.close()
            raise ExtractionError(f"{self.name}: HTTP {response.status_code}")
        return response

    def _verify(self, response: requests.Response, run: Run) -> None:
        for check in self.expect:
            if not check.holds(response):
                raise ExtractionError(f"{self.name}: {check}")
        for key, capture in self.capture.items():
            value = capture.extract(response, run.session)
            if value is None:
                if capture.default is not None:
                    value = capture.default
                elif not capture.required:
                    continue
                else:
                    raise ExtractionError(f"{self.name}: captura {key!r} não encontrada")
            run.context[key] = str(value)


@dataclass(frozen=True)
class Download(Request):
    """A requisição que traz o arquivo: o corpo vai em stream para `filename`.

    `content_types`, quando dado, são os prefixos aceitos de `Content-Type`: uma
    página de erro no lugar do arquivo falha o passo, em vez de virar dado.
    """

    filename: str = "download"
    content_types: tuple[str, ...] = ()

    def execute(self, run: Run) -> RawFile | None:
        response = self._send(run, stream=True)
        try:
            content_type = response.headers.get("Content-Type", "").lower()
            if self.content_types and not content_type.startswith(self.content_types):
                raise ExtractionError(
                    f"{self.name}: Content-Type {content_type or 'ausente'} "
                    f"(esperado: {', '.join(self.content_types)})"
                )
            self._reject_body_checks()
            return write_stream(
                response.iter_content(CHUNK_BYTES),
                run.work_dir / self.filename,
                self.filename,
            )
        finally:
            response.close()

    def _reject_body_checks(self) -> None:
        if self.expect or self.capture:
            raise ExtractionError(
                f"{self.name}: Download não lê o corpo; capture em outro passo"
            )


@dataclass(frozen=True)
class SetHeaders:
    """Acrescenta ou troca cabeçalhos da sessão (valem para os passos seguintes)."""

    headers: Mapping[str, str]

    def execute(self, run: Run) -> RawFile | None:
        run.session.headers.update(render(dict(self.headers), run.context))
        return None


@dataclass(frozen=True)
class DropHeaders:
    """Tira cabeçalhos da sessão."""

    names: tuple[str, ...]

    def execute(self, run: Run) -> RawFile | None:
        for name in self.names:
            run.session.headers.pop(name, None)
        return None
