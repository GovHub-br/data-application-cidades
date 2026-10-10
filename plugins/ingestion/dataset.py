"""DatasetSpec: o que uma DAG declara sobre o seu dataset, só com literais."""

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from ingestion.converters.config_converter import ConverterConfig
from ingestion.extractors.base_extractor import RawFile
from ingestion.extractors.config_extractor import ExtractorConfig
from ingestion.layout import safe_segment
from ingestion.loaders.load_types import LoadMode, validate_load

#: Configuração literal, ou função que a monta dentro da task (lendo uma Variable,
#: por exemplo), para que nada seja consultado no parse da DAG.
ExtractorSource = ExtractorConfig | Callable[[], ExtractorConfig]


class Prepare(Protocol):
    """Transformação de um arquivo extraído antes do pouso na raw.

    Recebe uma parte já em disco e cede as partes que a substituem (zero, uma ou
    várias: um zip vira os membros), apagando a de entrada. Uma parte por vez.
    """

    def apply(self, part: RawFile, work_dir: Path) -> Iterator[RawFile]:
        """As partes que substituem `part`."""


@dataclass(frozen=True)
class DatasetSpec:
    """Um dataset do lake: de onde vem, como converter e como carregar no bronze.

    `domain` e `dataset` viram pastas (`raw/<domain>/<dataset>/...`) e já precisam ser
    segmentos seguros. `load_mode`/`keys` seguem `validate_load`; o mesmo modo vai no
    `meta.load_mode` da fonte no dbt, que é quem carrega o bronze.

    `incremental`: a extração recebe os `source_id`s que já pousaram (lidos dos
    manifestos da raw) e só traz o que é novo. `prepare`: transformações aplicadas a
    cada arquivo extraído antes do pouso (descompactar, mascarar PII...), em ordem.
    """

    domain: str
    dataset: str
    extractor: ExtractorSource
    converter: ConverterConfig = field(default_factory=ConverterConfig)
    load_mode: LoadMode | str = LoadMode.OVERWRITE
    keys: Sequence[str] = ()
    incremental: bool = False
    prepare: Sequence[Prepare] = ()

    def __post_init__(self) -> None:
        for name in (self.domain, self.dataset):
            if not name or safe_segment(name) != name:
                raise ValueError(
                    f"domain/dataset precisa ser um segmento seguro: {name!r}"
                )
        object.__setattr__(self, "keys", validate_load(self.load_mode, self.keys))
        object.__setattr__(self, "load_mode", LoadMode(self.load_mode))

    def extractor_config(self) -> ExtractorConfig:
        """A configuração do extrator; se for função, só é chamada aqui (na task)."""
        if isinstance(self.extractor, ExtractorConfig):
            return self.extractor
        return self.extractor()
