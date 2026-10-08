"""Pouso de arquivos numa partição do storage, uma parte por vez."""

import json
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ingestion.storage.base_storage import StorageBackend

#: Gravado por último: partição sem ele é ingestão incompleta e não é lida adiante.
SUCCESS_MARKER = "_SUCCESS"


class Part(Protocol):
    """Arquivo local a pousar (o RawFile dos extratores satisfaz)."""

    @property
    def name(self) -> str: ...

    @property
    def path(self) -> Path: ...

    @property
    def size(self) -> int: ...

    @property
    def sha256(self) -> str: ...


@dataclass(frozen=True)
class LandingResult:
    prefix: str
    keys: tuple[str, ...]
    total_bytes: int


def land(storage: StorageBackend, parts: Iterable[Part], prefix: str) -> LandingResult:
    """Sobe cada parte para `prefix + name` e apaga a cópia local antes da próxima.

    Consome o gerador do extrator, então nem a memória nem o disco do worker guardam
    mais de uma parte. Só depois da última grava `_SUCCESS`, um manifesto com nome,
    tamanho e sha256 de cada arquivo: uma falha no meio deixa a partição sem
    marcador, e ela não vira "a última ingestão". Sem partes, não grava nada.
    """
    if not prefix.endswith("/"):
        raise ValueError(f"o prefixo da partição deve terminar em '/': {prefix!r}")
    manifest: list[dict[str, object]] = []
    keys: list[str] = []
    names: set[str] = set()
    total_bytes = 0
    for part in parts:
        if part.name == SUCCESS_MARKER or part.name in names:
            raise ValueError(f"nome repetido ou reservado na ingestão: {part.name!r}")
        key = prefix + part.name
        storage.put_file(key, part.path)
        part.path.unlink()
        names.add(part.name)
        keys.append(key)
        total_bytes += part.size
        manifest.append({"name": part.name, "size": part.size, "sha256": part.sha256})
    if keys:
        _mark_success(storage, prefix, manifest)
    return LandingResult(prefix=prefix, keys=tuple(keys), total_bytes=total_bytes)


def _mark_success(
    storage: StorageBackend, prefix: str, manifest: list[dict[str, object]]
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        marker = Path(tmp) / SUCCESS_MARKER
        marker.write_text(json.dumps({"files": manifest}, ensure_ascii=False))
        storage.put_file(prefix + SUCCESS_MARKER, marker)
