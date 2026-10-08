"""Contrato da extração: o dado da fonte vira arquivo em disco, no formato original."""

import hashlib
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ingestion.extractors.config_extractor import ExtractorConfig
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


class Extractor(ABC):
    """Strategy de extração: copia o dado de uma fonte para disco, como veio.

    `extract` é um gerador: grava uma parte, cede o RawFile e só grava a próxima
    quando quem consome (a RawLanding) já subiu e apagou a anterior. Assim nem a
    memória nem o disco do worker dependem do tamanho da fonte.

    `ingestion_time` é o instante da ingestão (com fuso): o dia do e-mail, o fim de
    uma janela de datas. Sem singleton: cada execução tem a sua instância.
    """

    def __init__(self, config: ExtractorConfig, ingestion_time: datetime) -> None:
        self.config = config
        self.ingestion_time = ingestion_time

    @classmethod
    def from_config(
        cls, config: ExtractorConfig, ingestion_time: datetime
    ) -> "Extractor":
        """Constrói a estratégia; sobrescreva para validar os campos que ela usa."""
        return cls(config, ingestion_time)

    @abstractmethod
    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        """Grava as partes da fonte sob `work_dir`, uma por vez."""


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
