"""Fábrica de conversores (registro por formato e extensão)."""

from collections.abc import Callable, Iterable
from pathlib import Path

from ingestion.converters.base_converter import FileConverter
from ingestion.converters.config_converter import ConverterConfig


class ConverterFactory:
    """Escolhe o conversor de um arquivo: `config.format`, senão a extensão.

    Formato novo se registra com `@ConverterFactory.register("<nome>", extensions=...)`
    na própria classe; nada aqui muda para acrescentá-lo.
    """

    _registry: dict[str, type[FileConverter]] = {}
    _extensions: dict[str, str] = {}

    @classmethod
    def register(
        cls, name: str, extensions: Iterable[str] = ()
    ) -> Callable[[type[FileConverter]], type[FileConverter]]:
        def decorator(converter_cls: type[FileConverter]) -> type[FileConverter]:
            cls._registry[name] = converter_cls
            for extension in extensions:
                cls._extensions[extension.lower()] = name
            return converter_cls

        return decorator

    @classmethod
    def for_file(cls, path: Path, config: ConverterConfig) -> FileConverter:
        name = config.format or cls._extensions.get(path.suffix.lower())
        if name not in cls._registry:
            raise ValueError(
                f"formato desconhecido para {path.name!r}: {name or path.suffix!r}. "
                f"Registrados: {cls._describe()}"
            )
        return cls._registry[name](config)

    @classmethod
    def _describe(cls) -> str:
        """Formatos e extensões registrados, para a mensagem de erro."""
        parts = []
        for fmt in sorted(cls._registry):
            extensions = sorted(e for e, f in cls._extensions.items() if f == fmt)
            parts.append(f"{fmt} ({', '.join(extensions)})")
        return ", ".join(parts)
