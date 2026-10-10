"""O que é próprio do extrator `http_file`, além do contrato."""

import hashlib
import tracemalloc
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from requests.adapters import HTTPAdapter

from ingestion.extractors import (
    ExtractorConfig,
    ExtractorFactory,
    HttpRequest,
    RawFile,
)
from tests.ingestion.extractors.conftest import WHEN, FakeHttpServer, Route


def _download(tmp_path: Path, request: HttpRequest, adapter: Any = None) -> RawFile:
    config = ExtractorConfig(
        source="http_file", conn_id="http_test", requests=(request,), adapter=adapter
    )
    [part] = ExtractorFactory.create(config, ingestion_time=WHEN).extract(tmp_path)
    return part


@pytest.mark.parametrize(
    ("disposition", "expected"),
    [
        ('attachment; filename="INCC-M série.xlsx"', "INCC-M_s_rie.xlsx"),
        ("attachment; filename*=UTF-8''s%C3%A9rie%202026.csv", "s_rie_2026.csv"),
    ],
)
def test_name_comes_from_content_disposition(
    http_server: FakeHttpServer, tmp_path: Path, disposition: str, expected: str
) -> None:
    http_server.routes["/download"] = Route(
        body=b"x", headers={"Content-Disposition": disposition}
    )

    part = _download(tmp_path, HttpRequest(name="incc_m", endpoint="/download"))

    assert part.name == expected


def test_name_falls_back_to_request_name_without_file_in_url(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    http_server.routes["/download/"] = Route(body=b"x")

    part = _download(tmp_path, HttpRequest(name="incc_m", endpoint="/download/"))

    assert part.name == "incc_m"


def test_resolver_discovers_the_link_including_absolute_urls(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    # Como a ABECIP: a página da edição aponta para o arquivo, que muda de nome.
    base = f"http://127.0.0.1:{http_server.port}"
    http_server.routes["/indicadores"] = Route(
        body=f'<a href="{base}/arquivos/poupanca_2026_09.xlsx">baixar</a>'.encode()
    )
    http_server.routes["/arquivos/poupanca_2026_09.xlsx"] = Route(body=b"planilha")

    def find_link(hook: Any) -> str:
        page = hook.run("/indicadores").text
        return str(page.split('href="')[1].split('"')[0])

    part = _download(
        tmp_path,
        HttpRequest(name="poupanca", endpoint="/indicadores", resolve=find_link),
    )

    assert (part.name, part.path.read_bytes()) == ("poupanca_2026_09.xlsx", b"planilha")


def test_custom_adapter_is_mounted_on_the_session(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    # É por aqui que entra o TLS legado da FGV.
    class CountingAdapter(HTTPAdapter):
        sent = 0

        def send(self, *args: Any, **kwargs: Any) -> Any:
            CountingAdapter.sent += 1
            return super().send(*args, **kwargs)

    http_server.routes["/f.csv"] = Route(body=b"a;b\n")

    _download(
        tmp_path, HttpRequest(name="f", endpoint="/f.csv"), adapter=CountingAdapter()
    )

    assert CountingAdapter.sent == 1


def test_large_file_streams_with_constant_memory(
    http_server: FakeHttpServer, tmp_path: Path
) -> None:
    # 128 MiB atravessam o extrator; a memória Python não pode crescer junto.
    block = bytes(range(256)) * 4096  # 1 MiB
    blocks = 128

    def body() -> Iterator[bytes]:
        for n in range(blocks):
            yield n.to_bytes(4, "big") + block[4:]

    expected = hashlib.sha256()
    for chunk in body():
        expected.update(chunk)
    http_server.routes["/grande.csv"] = Route(body=body)

    tracemalloc.start()
    try:
        part = _download(tmp_path, HttpRequest(name="g", endpoint="/grande.csv"))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert part.size == blocks * len(block)
    assert part.sha256 == expected.hexdigest()
    assert peak < 32 * 1024 * 1024, f"pico de {peak / 2**20:.1f} MiB"
