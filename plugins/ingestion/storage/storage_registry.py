"""Fábrica de backends de storage (registro por nome)."""

from collections.abc import Callable
from typing import Any

from ingestion.storage.base_storage import StorageBackend


class StorageFactory:
    """Constrói um StorageBackend pelo nome com que ele se registrou.

    Backend novo se registra com `@StorageFactory.register("<nome>")` na própria
    classe; nada aqui muda para acrescentá-lo.
    """

    _registry: dict[str, type[StorageBackend]] = {}

    @classmethod
    def register(
        cls, name: str
    ) -> Callable[[type[StorageBackend]], type[StorageBackend]]:
        def decorator(backend_cls: type[StorageBackend]) -> type[StorageBackend]:
            cls._registry[name] = backend_cls
            return backend_cls

        return decorator

    @classmethod
    def create(cls, name: str, **kwargs: Any) -> StorageBackend:
        try:
            backend_cls = cls._registry[name]
        except KeyError:
            registered = ", ".join(sorted(cls._registry))
            raise ValueError(
                f"storage desconhecido: {name!r}. Registrados: {registered}"
            ) from None
        return backend_cls(**kwargs)
