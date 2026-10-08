"""Contrato da extração: o dado da fonte vira arquivo em disco, no formato original."""

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from ingestion.layout import safe_segment


@dataclass(frozen=True)
class RawFile:
    """Uma parte extraída, já em disco, pronta para pousar na raw.

    `name` é o nome do arquivo dentro da partição da raw; `sha256` e `size` são do
    que a fonte entregou, calculados na gravação, sem reler o arquivo.
    """

    name: str
    path: Path
    size: int
    sha256: str


def write_stream(chunks: Iterable[bytes], path: Path, name: str | None = None) -> RawFile:
    """Grava os blocos em `path` à medida que chegam e devolve o RawFile.

    Nenhum bloco é transformado: o arquivo é byte a byte o que a fonte mandou. Só um
    bloco fica em memória por vez, e cada um já está no disco antes do próximo.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    with path.open("wb") as target:
        for chunk in chunks:
            if not chunk:
                continue
            target.write(chunk)
            target.flush()
            digest.update(chunk)
            size += len(chunk)
    return RawFile(
        name=safe_segment(name or path.name),
        path=path,
        size=size,
        sha256=digest.hexdigest(),
    )
