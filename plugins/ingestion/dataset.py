"""DatasetSpec: o que uma DAG declara sobre o seu dataset."""

import dataclasses
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from ingestion.converters.config_converter import ConverterConfig
from ingestion.extractors.base_extractor import RawFile
from ingestion.extractors.builders import Entry
from ingestion.extractors.config_extractor import ExtractorConfig
from ingestion.layout import safe_segment
from ingestion.loaders.load_types import LoadMode, validate_load

#: Configuração literal, ou função da entrada que a monta dentro da task (a entrada
#: vem da Variable que a task leu; vazia numa DAG de um dataset só), para que nada
#: seja consultado no parse da DAG.
ExtractorSource = ExtractorConfig | Callable[[Entry], ExtractorConfig]


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

    `domain` e `dataset` viram pastas (`raw/<domain>/<dataset>/...`) e precisam ser
    segmentos seguros. `load_mode`/`keys` seguem `validate_load`; o mesmo modo vai no
    `meta.load_mode` da fonte no dbt, que é quem carrega o bronze.

    `incremental`: a extração recebe os `source_id`s que já pousaram (lidos dos
    manifestos da raw) e só traz o que é novo. `prepare`: transformações aplicadas a
    cada arquivo extraído antes do pouso (descompactar, mascarar PII...), em ordem.

    Numa DAG com vários datasets (uma lista numa Variable), `dataset`, `extractor` e
    `prepare` podem ser funções da entrada (a mecânica da fonte, no perfil):
    `resolve(entrada)` devolve o spec concreto de cada uma.
    """

    domain: str
    dataset: str | Callable[[Entry], str]
    extractor: ExtractorSource
    converter: ConverterConfig = field(default_factory=ConverterConfig)
    load_mode: LoadMode | str = LoadMode.OVERWRITE
    keys: Sequence[str] = ()
    incremental: bool = False
    prepare: Sequence[Prepare] | Callable[[Entry], Sequence[Prepare]] = ()

    def __post_init__(self) -> None:
        names = [self.domain] + ([self.dataset] if isinstance(self.dataset, str) else [])
        for name in names:
            if not name or safe_segment(name) != name:
                raise ValueError(
                    f"domain/dataset precisa ser um segmento seguro: {name!r}"
                )
        object.__setattr__(self, "keys", validate_load(self.load_mode, self.keys))
        object.__setattr__(self, "load_mode", LoadMode(self.load_mode))

    def resolve(self, entry: Entry | None = None) -> "DatasetSpec":
        """O spec concreto: chama as funções da entrada (dentro da task)."""
        entry = entry or {}
        if not entry and callable(self.dataset):
            raise ValueError(
                "este DatasetSpec depende de uma entrada (a lista da Variable da DAG)"
            )
        dataset = self.dataset(entry) if callable(self.dataset) else self.dataset
        extractor = (
            self.extractor
            if isinstance(self.extractor, ExtractorConfig)
            else self.extractor(entry)
        )
        prepare = self.prepare(entry) if callable(self.prepare) else self.prepare
        return dataclasses.replace(
            self, dataset=dataset, extractor=extractor, prepare=tuple(prepare)
        )

    @property
    def name(self) -> str:
        """O nome do dataset (a pasta no lake); só existe no spec resolvido."""
        if not isinstance(self.dataset, str):
            raise ValueError("DatasetSpec não resolvido: chame resolve(entrada)")
        return self.dataset

    def extractor_config(self) -> ExtractorConfig:
        """A configuração do extrator; se for função, só é chamada aqui (na task)."""
        if isinstance(self.extractor, ExtractorConfig):
            return self.extractor
        return self.extractor({})
