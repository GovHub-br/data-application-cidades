"""DatasetSpec: o que uma DAG declara sobre o seu dataset, só com literais."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from ingestion.converters.config_converter import ConverterConfig
from ingestion.extractors.config_extractor import ExtractorConfig
from ingestion.layout import safe_segment
from ingestion.loaders.load_types import LoadMode, validate_load

#: Configuração literal, ou função que a monta dentro da task (lendo uma Variable,
#: por exemplo), para que nada seja consultado no parse da DAG.
ExtractorSource = ExtractorConfig | Callable[[], ExtractorConfig]


@dataclass(frozen=True)
class DatasetSpec:
    """Um dataset do lake: de onde vem, como converter e como carregar no bronze.

    `domain` e `dataset` viram pastas (`raw/<domain>/<dataset>/...`) e já precisam ser
    segmentos seguros. `load_mode`/`keys` seguem `validate_load`; o mesmo modo vai no
    `meta.load_mode` da fonte no dbt, que é quem carrega o bronze.
    """

    domain: str
    dataset: str
    extractor: ExtractorSource
    converter: ConverterConfig = field(default_factory=ConverterConfig)
    load_mode: LoadMode | str = LoadMode.OVERWRITE
    keys: Sequence[str] = ()

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
