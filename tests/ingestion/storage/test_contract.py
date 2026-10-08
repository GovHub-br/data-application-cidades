"""Contrato de todo StorageBackend.

Caminhos relativos à raiz (diretório ou bucket), operações sempre de arquivo em
disco, semântica de prefixo do S3 na listagem. Todo backend da lista em
`conftest.BACKENDS` herda estes testes.
"""

from pathlib import Path

import pytest

from ingestion.storage import ObjectNotFoundError, StorageBackend


def _file(tmp_path: Path, name: str, data: bytes) -> Path:
    path = tmp_path / "local" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def test_put_file_then_get_file_roundtrips_bytes(
    backend: StorageBackend, tmp_path: Path
) -> None:
    payload = bytes(range(256)) * 4096  # 1 MiB binário, com todos os bytes
    backend.put_file("raw/ibge/sinapi/a.bin", _file(tmp_path, "a.bin", payload))

    target = tmp_path / "out" / "nested" / "a.bin"
    backend.get_file("raw/ibge/sinapi/a.bin", target)

    assert target.read_bytes() == payload


def test_put_file_overwrites_existing_object(
    backend: StorageBackend, tmp_path: Path
) -> None:
    backend.put_file("x.txt", _file(tmp_path, "v1.txt", b"v1"))
    backend.put_file("x.txt", _file(tmp_path, "v2.txt", b"v2"))

    target = tmp_path / "x.txt"
    backend.get_file("x.txt", target)

    assert target.read_bytes() == b"v2"


def test_exists_reflects_put_and_delete(backend: StorageBackend, tmp_path: Path) -> None:
    assert not backend.exists("x.txt")

    backend.put_file("x.txt", _file(tmp_path, "x.txt", b"x"))
    assert backend.exists("x.txt")

    backend.delete("x.txt")
    assert not backend.exists("x.txt")


def test_get_file_of_missing_object_raises(
    backend: StorageBackend, tmp_path: Path
) -> None:
    with pytest.raises(ObjectNotFoundError):
        backend.get_file("nao/existe.txt", tmp_path / "out.txt")


def test_delete_of_missing_object_raises(backend: StorageBackend) -> None:
    with pytest.raises(ObjectNotFoundError):
        backend.delete("nao/existe.txt")


def test_list_returns_sorted_keys_under_prefix(
    backend: StorageBackend, tmp_path: Path
) -> None:
    source = _file(tmp_path, "f", b"1")
    for key in [
        "raw/ibge/sinapi/2026-10-08/060000/b.json",
        "raw/ibge/sinapi/2026-10-08/060000/a.json",
        "raw/ibge/sinapi/2026-10-09/060000/a.json",
        "raw/ibge/pib/2026-10-08/060000/a.json",
        "staging/ibge/sinapi/2026-10-08/060000/part-0.parquet",
    ]:
        backend.put_file(key, source)

    assert backend.list("raw/ibge/sinapi/") == [
        "raw/ibge/sinapi/2026-10-08/060000/a.json",
        "raw/ibge/sinapi/2026-10-08/060000/b.json",
        "raw/ibge/sinapi/2026-10-09/060000/a.json",
    ]


def test_list_uses_string_prefix_semantics(
    backend: StorageBackend, tmp_path: Path
) -> None:
    # Como no S3: "raw/ibge/" não pega "raw/ibge2/", mas "raw/ibge" pega os dois.
    source = _file(tmp_path, "f", b"1")
    backend.put_file("raw/ibge/a.json", source)
    backend.put_file("raw/ibge2/a.json", source)

    assert backend.list("raw/ibge/") == ["raw/ibge/a.json"]
    assert backend.list("raw/ibge") == ["raw/ibge/a.json", "raw/ibge2/a.json"]


def test_list_of_missing_prefix_is_empty(backend: StorageBackend) -> None:
    assert backend.list("raw/nada/") == []
