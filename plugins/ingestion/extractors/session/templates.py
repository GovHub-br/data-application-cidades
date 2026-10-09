"""Modelos `{nome}` nos passos: valores capturados e Variables do fluxo."""

from collections.abc import Mapping
from typing import Any

from ingestion.extractors.extractor_errors import ExtractionError


class _Context(dict[str, str]):
    def __missing__(self, key: str) -> str:
        raise ExtractionError(f"{{{key}}} sem valor: nenhum passo capturou {key!r}")


def render(value: Any, context: Mapping[str, str]) -> Any:
    """Preenche `{nome}` em textos, também dentro de dicionários e listas.

    Só texto é modelo (`{{` vira `{`); números, booleanos e nulos passam como estão.
    """
    if isinstance(value, str):
        return value.format_map(_Context(context))
    if isinstance(value, Mapping):
        return {key: render(item, context) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(render(item, context) for item in value)
    return value
