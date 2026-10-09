"""Preparo `Unpack`: zip e gzip viram os arquivos de dentro, pelo conteúdo."""

import dataclasses
import gzip
import posixpath
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from ingestion.extractors import RawFile, write_stream

CHUNK_BYTES = 1024 * 1024
ZIP_MAGIC = b"PK\x03\x04"
GZIP_MAGIC = b"\x1f\x8b"
COMPRESSION_SUFFIX = re.compile(r"\.(zip|gz|gzip)$", re.IGNORECASE)


@dataclass(frozen=True)
class Unpack:
    """Descompacta pelo conteúdo, não pela extensão (há `.zip` que é gzip).

    - zip: cada membro que casa com `members` (regex no nome) vira um arquivo, um
      por vez, em fluxo; nenhum membro casando é erro, salvo com
      `require_match=False` (pacote com várias famílias, em que a procurada pode
      não estar);
    - gzip: o único membro, com o nome do cabeçalho do gzip (ou o nome do arquivo
      sem `.gz`/`.zip`);
    - outro formato: passa como está.

    `prefix_with_archive` antepõe o nome do arquivo compactado (`<zip>__<membro>`),
    para membros de nome repetido entre entregas (o mesmo `.mdb` em todo zip). O
    `source_id` da entrega segue para os membros; o nome dela vai em `details`.
    """

    members: str = r".*"
    prefix_with_archive: bool = False
    require_match: bool = True

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        with part.path.open("rb") as head:
            magic = head.read(4)
        if magic.startswith(ZIP_MAGIC):
            yield from self._zip(part, work_dir)
        elif magic.startswith(GZIP_MAGIC):
            yield from self._gzip(part, work_dir)
        else:
            yield part
            return
        part.path.unlink(missing_ok=True)

    def _zip(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        pattern = re.compile(self.members)
        found = False
        with zipfile.ZipFile(part.path) as archive:
            for info in archive.infolist():
                name = posixpath.basename(info.filename)
                if info.is_dir() or not pattern.search(name):
                    continue
                found = True
                with archive.open(info) as member:
                    yield self._member(
                        part, name, iter(lambda: member.read(CHUNK_BYTES), b""), work_dir
                    )
        if not found and self.require_match:
            raise ValueError(f"{part.name}: nenhum membro casa {self.members!r}")

    def _gzip(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        name = _gzip_name(part.path) or COMPRESSION_SUFFIX.sub("", part.name)
        with gzip.open(part.path, "rb") as member:
            yield self._member(
                part, name, iter(lambda: member.read(CHUNK_BYTES), b""), work_dir
            )

    def _member(
        self, part: RawFile, name: str, chunks: Iterator[bytes], work_dir: Path
    ) -> RawFile:
        if self.prefix_with_archive:
            name = f"{COMPRESSION_SUFFIX.sub('', part.name)}__{name}"
        work_dir.mkdir(parents=True, exist_ok=True)
        out = write_stream(chunks, work_dir / name, name)
        return dataclasses.replace(
            out,
            source_id=part.source_id,
            details={**part.details, "archive": part.name},
        )


def _gzip_name(path: Path) -> str | None:
    """Nome original no cabeçalho do gzip (flag FNAME), se houver."""
    with path.open("rb") as raw:
        header = raw.read(10)
        flags = header[3]
        if flags & 0x04:  # FEXTRA
            size = int.from_bytes(raw.read(2), "little")
            raw.read(size)
        if not flags & 0x08:  # FNAME
            return None
        name = bytearray()
        while (byte := raw.read(1)) not in (b"", b"\x00"):
            name += byte
    return posixpath.basename(name.decode("latin-1")) or None
