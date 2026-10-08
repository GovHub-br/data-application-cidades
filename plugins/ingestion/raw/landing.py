"""Pouso na raw: as partes extraídas vão para a partição da ingestão, uma por vez."""

import json
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from ingestion.extractors.base_extractor import RawFile
from ingestion.storage.base_storage import StorageBackend

#: Gravado por último: partição sem ele é ingestão incompleta e não é lida adiante.
SUCCESS_MARKER = "_SUCCESS"


@dataclass(frozen=True)
class LandingResult:
    prefix: str
    keys: tuple[str, ...]
    total_bytes: int


class RawLanding:
    """Sobe cada RawFile para `prefix + name` e apaga a cópia local antes da próxima.

    Como consome o gerador do extrator, nem a memória nem o disco do worker guardam
    mais de uma parte. Só depois da última parte grava `_SUCCESS`, um manifesto com
    nome, tamanho e sha256 de cada arquivo: uma falha no meio deixa a partição sem
    marcador, e ela não vira "a última ingestão". Sem partes, não grava nada.
    """

    def __init__(self, storage: StorageBackend) -> None:
        self.storage = storage

    def land(self, parts: Iterable[RawFile], prefix: str) -> LandingResult:
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
            self.storage.put_file(key, part.path)
            part.path.unlink()
            names.add(part.name)
            keys.append(key)
            total_bytes += part.size
            manifest.append({"name": part.name, "size": part.size, "sha256": part.sha256})
        if keys:
            self._mark_success(prefix, manifest)
        return LandingResult(prefix=prefix, keys=tuple(keys), total_bytes=total_bytes)

    def _mark_success(self, prefix: str, manifest: list[dict[str, object]]) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / SUCCESS_MARKER
            marker.write_text(json.dumps({"files": manifest}, ensure_ascii=False))
            self.storage.put_file(prefix + SUCCESS_MARKER, marker)
