"""Storage que enxerga só uma pasta de outro storage, como se fosse a raiz."""

from pathlib import Path

from ingestion.storage.base_storage import StorageBackend

#: Camadas reais do lake: um prefixo nunca pode ser uma delas, senão a "pasta de
#: teste" seria o próprio lake.
_LAKE_LAYERS = {"raw", "staging"}


class PrefixedStorage(StorageBackend):
    """Prefixa toda chave com `prefix`; a listagem devolve as chaves sem ele.

    Serve para rodar o pipeline de verdade isolado numa pasta (`tests/` do MinIO de
    dev) sem tocar em `raw/` e `staging/` do lake. Não é registrado na fábrica:
    embrulha o backend que a fábrica criou.
    """

    def __init__(self, inner: StorageBackend, prefix: str) -> None:
        clean = prefix.strip("/")
        if not clean or clean.split("/")[0] in _LAKE_LAYERS:
            raise ValueError(f"prefixo inválido (vazio, raw/ ou staging/): {prefix!r}")
        self.inner = inner
        self.prefix = f"{clean}/"

    def put_file(self, key: str, local_path: Path) -> None:
        self.inner.put_file(self.prefix + key, local_path)

    def get_file(self, key: str, local_path: Path) -> None:
        self.inner.get_file(self.prefix + key, local_path)

    def list(self, prefix: str) -> list[str]:
        return [key[len(self.prefix) :] for key in self.inner.list(self.prefix + prefix)]

    def delete(self, key: str) -> None:
        self.inner.delete(self.prefix + key)

    def copy(self, src_key: str, dst_key: str) -> None:
        self.inner.copy(self.prefix + src_key, self.prefix + dst_key)

    def exists(self, key: str) -> bool:
        return self.inner.exists(self.prefix + key)
