"""Pouso de arquivos numa partição do storage, uma parte por vez."""

import json
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ingestion.storage.base_storage import StorageBackend
from ingestion.storage.storage_errors import StorageError

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


def land(
    storage: StorageBackend,
    parts: Iterable[Part],
    prefix: str,
    details: Callable[[Part], Mapping[str, object]] | None = None,
    summary: Callable[[], Mapping[str, object]] | None = None,
) -> LandingResult:
    """Sobe cada parte para `prefix + name` e apaga a cópia local antes da próxima.

    Consome o gerador do extrator, então nem a memória nem o disco do worker guardam
    mais de uma parte. Só depois da última grava `_SUCCESS`, um manifesto com nome,
    tamanho e sha256 de cada arquivo (mais o que `details` devolver para a parte),
    e no topo o que `summary` devolver depois da última parte: uma falha no meio
    deixa a partição sem marcador, e ela não vira "a última ingestão". Sem partes,
    não grava nada.
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
        entry: dict[str, object] = {
            "name": part.name,
            "size": part.size,
            "sha256": part.sha256,
        }
        if details is not None:
            entry.update(details(part))
        manifest.append(entry)
    if keys:
        _mark_success(storage, prefix, manifest, summary() if summary else {})
    return LandingResult(prefix=prefix, keys=tuple(keys), total_bytes=total_bytes)


def _mark_success(
    storage: StorageBackend,
    prefix: str,
    manifest: list[dict[str, object]],
    summary: Mapping[str, object],
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        marker = Path(tmp) / SUCCESS_MARKER
        marker.write_text(json.dumps({**summary, "files": manifest}, ensure_ascii=False))
        storage.put_file(prefix + SUCCESS_MARKER, marker)


def publish_latest(
    storage: StorageBackend, partition_prefix: str, latest_prefix: str
) -> list[str]:
    """Espelha uma partição completa em `latest_prefix`, que é o que o bronze lê.

    Os arquivos vão para `latest/<AAAA-MM-DD>/<HHMMSS>/`, com a partição de origem
    no caminho: o `filename` que o bronze lê traz a data da ingestão, como nos
    modos que leem as partições direto (o `dt_ingest` da prata sai dele).

    Só publica partição com `_SUCCESS`. Copia os arquivos novos no servidor,
    depois remove do `latest/` o que não veio nesta ingestão (inclusive a cópia da
    partição anterior), e grava o `_SUCCESS` por último, na raiz do `latest/`: o
    manifesto da partição mais `particao` (a publicada) e `antecessor` (a que ela
    substituiu, que continua inteira no lugar dela; nula na primeira publicação). O
    `latest/` nunca fica vazio; um leitor no meio da troca pode ver arquivos novos e
    antigos misturados, o que não acontece no fluxo atual (o dbt roda depois da
    ingestão).
    """
    if not storage.exists(partition_prefix + SUCCESS_MARKER):
        raise StorageError(
            f"partição sem {SUCCESS_MARKER}, não publicada: {partition_prefix}"
        )
    names = [
        key[len(partition_prefix) :]
        for key in storage.list(partition_prefix)
        if key != partition_prefix + SUCCESS_MARKER
    ]
    partition = "/".join(partition_prefix.rstrip("/").split("/")[-2:]) + "/"
    antecessor = _antecessor(storage, partition_prefix, latest_prefix)
    published = [latest_prefix + partition + name for name in names]
    for name, key in zip(names, published):
        storage.copy(partition_prefix + name, key)
    keep = set(published) | {latest_prefix + SUCCESS_MARKER}
    for key in storage.list(latest_prefix):
        if key not in keep:
            storage.delete(key)
    with tempfile.TemporaryDirectory() as tmp:
        marker = Path(tmp) / SUCCESS_MARKER
        storage.get_file(partition_prefix + SUCCESS_MARKER, marker)
        manifest = json.loads(marker.read_text() or "{}")
        manifest.update(particao=partition_prefix, antecessor=antecessor)
        marker.write_text(json.dumps(manifest, ensure_ascii=False))
        storage.put_file(latest_prefix + SUCCESS_MARKER, marker)
    return published


def _antecessor(
    storage: StorageBackend, partition_prefix: str, latest_prefix: str
) -> str | None:
    """A partição que o `latest/` publicava antes desta (a própria, se é a mesma)."""
    base = latest_prefix[: -len("latest/")]
    current = {
        "/".join(key[len(latest_prefix) :].split("/")[:2]) + "/"
        for key in storage.list(latest_prefix)
        if key != latest_prefix + SUCCESS_MARKER
    }
    previous = sorted(base + p for p in current)
    if previous and previous[-1] != partition_prefix:
        return previous[-1]
    if not storage.exists(latest_prefix + SUCCESS_MARKER):
        return None
    # republicação da mesma partição: a antecessora continua a de antes
    with tempfile.TemporaryDirectory() as tmp:
        marker = Path(tmp) / SUCCESS_MARKER
        storage.get_file(latest_prefix + SUCCESS_MARKER, marker)
        value = json.loads(marker.read_text() or "{}").get("antecessor")
    return str(value) if value else None
