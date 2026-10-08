"""Storage num diretório local: testes e ambientes sem MinIO."""

import shutil
from pathlib import Path

from ingestion.storage.base_storage import StorageBackend
from ingestion.storage.storage_errors import ObjectNotFoundError
from ingestion.storage.storage_registry import StorageFactory


@StorageFactory.register("local")
class LocalStorageBackend(StorageBackend):
    """Cada chave é um arquivo sob `root`; a listagem segue a semântica do S3."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put_file(self, key: str, local_path: Path) -> None:
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)

    def get_file(self, key: str, local_path: Path) -> None:
        source = self._existing(key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, local_path)

    def list(self, prefix: str) -> list[str]:
        # Só desce a partir do último diretório completo do prefixo; o resto do
        # prefixo é comparado como string, igual ao S3 ("raw/ibge" pega "raw/ibge2/").
        start = self._resolve(prefix.rpartition("/")[0]) if "/" in prefix else self.root
        if not start.is_dir():
            return []
        keys = (path.relative_to(self.root).as_posix() for path in start.rglob("*"))
        return sorted(
            key for key in keys if key.startswith(prefix) and self._resolve(key).is_file()
        )

    def delete(self, key: str) -> None:
        self._existing(key).unlink()

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def _existing(self, key: str) -> Path:
        path = self._resolve(key)
        if not path.is_file():
            raise ObjectNotFoundError(key)
        return path

    def _resolve(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if path != self.root and self.root not in path.parents:
            raise ValueError(f"chave fora da raiz do storage: {key!r}")
        return path
