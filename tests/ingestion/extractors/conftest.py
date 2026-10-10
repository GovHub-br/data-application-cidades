"""Fontes falsas dos extratores e os casos que alimentam o contrato.

O servidor HTTP é local, em thread, sem dependência nova; a Connection aponta para
ele por `AIRFLOW_CONN_HTTP_TEST`. Cada estratégia registra em CASES como montar um
extrator de sucesso, o que ele deve gravar, e um extrator cuja fonte não existe.
"""

import io
import re
import threading
import zipfile
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from ingestion.extractors import (
    Extractor,
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
    MailQuery,
)

WHEN = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)

Body = bytes | Callable[[], Iterable[bytes]]


@dataclass
class Route:
    """Resposta de uma rota: status, corpo (bytes ou gerador) e cabeçalhos.

    `fail_first` responde 503 nas primeiras N chamadas, para testar o retry.
    `html_first` responde 200 com uma página HTML nas primeiras N chamadas, como o
    SGS do BACEN faz quando recusa a requisição. `on_post` responde os POST do mesmo
    caminho (páginas ASP.NET respondem GET e POST no mesmo endereço).
    """

    body: Body = b""
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    fail_first: int = 0
    html_first: int = 0
    on_post: "Route | None" = None
    calls: int = 0
    requests: list[dict[str, Any]] = field(default_factory=list)


class FakeHttpServer:
    def __init__(self) -> None:
        self.routes: dict[str, Route] = {}
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                server._serve(self)

            def do_POST(self) -> None:
                server._serve(self)

            def log_message(self, *args: Any) -> None:
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )

    def _serve(self, handler: BaseHTTPRequestHandler) -> None:
        path, _, query = handler.path.partition("?")
        route = self.routes.get(path)
        length = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(length) if length else b""
        if route is None:
            handler.send_response(404)
            handler.end_headers()
            return
        if handler.command == "POST" and route.on_post is not None:
            route = route.on_post
        route.calls += 1
        route.requests.append(
            {
                "method": handler.command,
                "query": query,
                "body": body,
                "headers": dict(handler.headers),
            }
        )
        if route.calls <= route.fail_first:
            handler.send_response(503)
            handler.end_headers()
            return
        if route.calls <= route.fail_first + route.html_first:
            page = b"<html><title>Requisicao invalida!</title></html>"
            handler.send_response(200)
            handler.send_header("Content-Type", "text/html; charset=utf-8")
            handler.send_header("Content-Length", str(len(page)))
            handler.end_headers()
            handler.wfile.write(page)
            return
        handler.send_response(route.status)
        for name, value in route.headers.items():
            handler.send_header(name, value)
        if isinstance(route.body, bytes):
            handler.send_header("Content-Length", str(len(route.body)))
            handler.end_headers()
            handler.wfile.write(route.body)
        else:
            handler.end_headers()  # sem Content-Length: o corpo vai até fechar a conexão
            for chunk in route.body():
                handler.wfile.write(chunk)
        handler.close_connection = True

    def __enter__(self) -> "FakeHttpServer":
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def http_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeHttpServer]:
    with FakeHttpServer() as server:
        monkeypatch.setenv("AIRFLOW_CONN_HTTP_TEST", f"http://127.0.0.1:{server.port}")
        yield server


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry sem espera nos testes; o número de tentativas continua o de produção."""
    from ingestion.extractors.models import http_common

    monkeypatch.setattr(http_common, "RETRY_WAIT_SECONDS", 0)


@dataclass
class ContractCase:
    build: Callable[[], Extractor]
    expected: dict[str, bytes]
    build_missing: Callable[[], Extractor]


def _api_case(request: pytest.FixtureRequest) -> ContractCase:
    server: FakeHttpServer = request.getfixturevalue("http_server")
    server.routes["/serie/20704"] = Route(body=b'[{"data":"01/08/2026","valor":"5,1"}]')
    server.routes["/serie/20705"] = Route(body=b'[{"data":"01/08/2026","valor":"7,9"}]')

    def build(*paths: str) -> Extractor:
        config = ExtractorConfig(
            source="api",
            conn_id="http_test",
            requests=tuple(
                HttpRequest(name=p.rsplit("/", 1)[-1], endpoint=p) for p in paths
            ),
        )
        return ExtractorFactory.create(config, ingestion_time=WHEN)

    return ContractCase(
        build=lambda: build("/serie/20704", "/serie/20705"),
        expected={
            "20704.json": server.routes["/serie/20704"].body,  # type: ignore[dict-item]
            "20705.json": server.routes["/serie/20705"].body,  # type: ignore[dict-item]
        },
        build_missing=lambda: build("/serie/nao-existe"),
    )


def _http_file_case(request: pytest.FixtureRequest) -> ContractCase:
    server: FakeHttpServer = request.getfixturevalue("http_server")
    planilha = b"PK\x03\x04" + bytes(range(256)) * 64  # cabeçalho de zip/xlsx, binário
    server.routes["/arquivos/incc.xlsx"] = Route(body=planilha)

    def build(endpoint: str) -> Extractor:
        config = ExtractorConfig(
            source="http_file",
            conn_id="http_test",
            requests=(HttpRequest(name="incc_m", endpoint=endpoint),),
        )
        return ExtractorFactory.create(config, ingestion_time=WHEN)

    return ContractCase(
        build=lambda: build("/arquivos/incc.xlsx"),
        expected={"incc.xlsx": planilha},
        build_missing=lambda: build("/arquivos/nao-existe.xlsx"),
    )


class FakeImapHook:
    """ImapHook falso: guarda os anexos "da caixa" e registra como foi chamado."""

    attachments: dict[str, bytes] = {}
    calls: list[dict[str, Any]] = []

    def __init__(self, imap_conn_id: str) -> None:
        self.imap_conn_id = imap_conn_id
        self.connection: Any = None

    def get_connection(self, conn_id: str) -> Any:
        return f"<Connection {conn_id} do Airflow>"

    def __enter__(self) -> "FakeImapHook":
        # Como o ImapHook real: resolve a Connection ao conectar.
        self.connection = self.get_connection(self.imap_conn_id)
        return self

    def __exit__(self, *exc: Any) -> None:
        pass

    def download_mail_attachments(
        self, name: str, local_output_directory: str, **kwargs: Any
    ) -> None:
        FakeImapHook.calls.append(
            {
                "conn_id": self.imap_conn_id,
                "connection": self.connection,
                "name": name,
                **kwargs,
            }
        )
        for filename, payload in self.attachments.items():
            if re.match(name, filename):
                with open(f"{local_output_directory}/{filename}", "wb") as target:
                    target.write(payload)


def zip_of(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


@pytest.fixture
def fake_imap(monkeypatch: pytest.MonkeyPatch) -> type[FakeImapHook]:
    from ingestion.extractors.models import email_extractor

    monkeypatch.setattr(FakeImapHook, "attachments", {})
    monkeypatch.setattr(FakeImapHook, "calls", [])
    monkeypatch.setattr(
        email_extractor.EmailAttachmentExtractor, "hook_class", FakeImapHook
    )
    return FakeImapHook


def _email_case(request: pytest.FixtureRequest) -> ContractCase:
    hook: type[FakeImapHook] = request.getfixturevalue("fake_imap")
    tsv = "cabeçalho\tvalor\n1\t2\n".encode("utf-16")
    report = zip_of({"dotacao.csv": tsv})
    hook.attachments = {"dotacao_execucao.zip": report}

    def build(subject: str) -> Extractor:
        config = ExtractorConfig(
            source="email",
            conn_id="imap_test",
            mail=MailQuery(
                sender="tesouro@exemplo.gov.br",
                subject=subject,
                attachment_pattern=r".*\.zip$",
            ),
        )
        return ExtractorFactory.create(config, ingestion_time=WHEN)

    def build_missing() -> Extractor:
        hook.attachments = {}
        return build("assunto_sem_email")

    return ContractCase(
        build=lambda: build("dotacao_execucao_outras_fontes_mcid"),
        expected={"dotacao_execucao.zip": report},
        build_missing=build_missing,
    )


CASES: dict[str, Callable[[pytest.FixtureRequest], ContractCase]] = {
    "api": _api_case,
    "http_file": _http_file_case,
    "email": _email_case,
}


@pytest.fixture
def contract_case(request: pytest.FixtureRequest) -> ContractCase:
    return CASES[request.param](request)
