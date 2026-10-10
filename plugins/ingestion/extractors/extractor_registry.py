"""Fábrica de extratores (registro por nome da fonte)."""

from collections.abc import Callable
from datetime import datetime

from ingestion.extractors.base_extractor import Extractor
from ingestion.extractors.config_extractor import ExtractorConfig


class ExtractorFactory:
    """Constrói o Extractor da estratégia indicada em `config.source`.

    Estratégia nova se registra com `@ExtractorFactory.register("<nome>")` na
    própria classe; nada aqui muda para acrescentá-la.
    """

    _registry: dict[str, type[Extractor]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type[Extractor]], type[Extractor]]:
        def decorator(extractor_cls: type[Extractor]) -> type[Extractor]:
            cls._registry[name] = extractor_cls
            return extractor_cls

        return decorator

    @classmethod
    def create(cls, config: ExtractorConfig, *, ingestion_time: datetime) -> Extractor:
        if ingestion_time.tzinfo is None or ingestion_time.utcoffset() is None:
            raise ValueError(f"ingestion_time sem fuso horário: {ingestion_time!r}")
        try:
            extractor_cls = cls._registry[config.source]
        except KeyError:
            registered = ", ".join(sorted(cls._registry))
            raise ValueError(
                f"fonte de extração desconhecida: {config.source!r}. "
                f"Registradas: {registered}"
            ) from None
        return extractor_cls.from_config(config, ingestion_time)
