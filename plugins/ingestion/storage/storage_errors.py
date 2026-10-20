"""Erros do storage, independentes do backend."""


class StorageError(Exception):
    """Falha de storage que o chamador pode tratar sem conhecer o backend."""


class ObjectNotFoundError(StorageError):
    """O objeto pedido não existe no storage."""
