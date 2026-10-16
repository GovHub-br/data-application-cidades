"""O que é próprio do backend local e da fábrica de storage."""

from pathlib import Path

import pytest

from ingestion.storage import LocalStorageBackend, StorageFactory


def test_factory_builds_registered_backend(tmp_path: Path) -> None:
    backend = StorageFactory.create("local", root=tmp_path)

    assert isinstance(backend, LocalStorageBackend)


def test_factory_lists_registered_backends_on_unknown_name() -> None:
    with pytest.raises(ValueError, match="local"):
        StorageFactory.create("ftp")


def test_factory_builds_a_new_instance_per_call(tmp_path: Path) -> None:
    # Sem singleton: duas raízes diferentes não podem compartilhar instância.
    a = StorageFactory.create("local", root=tmp_path / "a")
    b = StorageFactory.create("local", root=tmp_path / "a")

    assert a is not b


@pytest.mark.parametrize("key", ["../fora.txt", "raw/../../fora.txt", "/etc/passwd"])
def test_local_rejects_keys_escaping_the_root(tmp_path: Path, key: str) -> None:
    backend = LocalStorageBackend(root=tmp_path / "lake")
    source = tmp_path / "f.txt"
    source.write_bytes(b"x")

    with pytest.raises(ValueError):
        backend.put_file(key, source)
