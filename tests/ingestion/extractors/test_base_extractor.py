"""RawFile e a gravação em stream que todo extrator usa."""

import dataclasses
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest

from ingestion.extractors import RawFile, describe_file, write_stream


def test_write_stream_keeps_bytes_and_reports_size_and_sha256(tmp_path: Path) -> None:
    chunks = [b"cabe\xc3\xa7alho;valor\n", b"", b"1;2\n", bytes(range(256))]
    expected = b"".join(chunks)

    raw = write_stream(chunks, tmp_path / "sub" / "dados.csv")

    assert raw.path.read_bytes() == expected
    assert raw.size == len(expected)
    assert raw.sha256 == hashlib.sha256(expected).hexdigest()
    assert raw.name == "dados.csv"


def test_write_stream_sanitizes_the_raw_name(tmp_path: Path) -> None:
    raw = write_stream([b"x"], tmp_path / "f.bin", name="série 20704.json")

    assert raw.name == "s_rie_20704.json"


def test_write_stream_consumes_chunks_lazily(tmp_path: Path) -> None:
    # Prova de streaming: cada bloco já está no disco antes do próximo ser pedido.
    target = tmp_path / "f.bin"
    seen_sizes = []

    def chunks() -> Iterator[bytes]:
        for block in [b"a" * 10, b"b" * 10, b"c" * 10]:
            seen_sizes.append(target.stat().st_size if target.exists() else 0)
            yield block

    write_stream(chunks(), target)

    assert seen_sizes == [0, 10, 20]


def test_raw_file_is_immutable(tmp_path: Path) -> None:
    raw = write_stream([b"x"], tmp_path / "f.bin")

    with pytest.raises(dataclasses.FrozenInstanceError):
        raw.size = 0  # type: ignore[misc]
    assert isinstance(raw, RawFile)


def test_describe_file_hashes_an_existing_file_in_chunks(tmp_path: Path) -> None:
    data = bytes(range(256)) * 1000
    path = tmp_path / "anexo do dia.zip"
    path.write_bytes(data)

    raw = describe_file(path, chunk_bytes=1000)

    assert (raw.name, raw.size, raw.path) == ("anexo_do_dia.zip", len(data), path)
    assert raw.sha256 == hashlib.sha256(data).hexdigest()
