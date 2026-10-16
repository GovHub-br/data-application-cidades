"""Interface do storage do lake."""

from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    """Strategy para o object storage do lake (MinIO, disco local, ...).

    Toda chave é relativa à raiz do backend (um diretório ou um bucket) e usa `/`
    como separador, como no S3. As operações movem arquivos em disco, nunca o dado
    inteiro em memória: é o que deixa o tamanho da fonte independente da memória do
    worker. Sem singleton: cada chamada da fábrica devolve uma instância nova.
    """

    @abstractmethod
    def put_file(self, key: str, local_path: Path) -> None:
        """Sobe o arquivo local para `key`, substituindo o objeto se ele existir."""

    @abstractmethod
    def get_file(self, key: str, local_path: Path) -> None:
        """Baixa `key` para `local_path`, criando os diretórios que faltarem.

        Levanta `ObjectNotFoundError` se o objeto não existir.
        """

    @abstractmethod
    def list(self, prefix: str) -> list[str]:
        """Chaves que começam com `prefix` (prefixo de string, como no S3), ordenadas."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Apaga `key`. Levanta `ObjectNotFoundError` se o objeto não existir."""

    @abstractmethod
    def copy(self, src_key: str, dst_key: str) -> None:
        """Copia `src_key` para `dst_key` dentro do storage, sem passar pelo worker.

        Substitui o destino se ele existir. Levanta `ObjectNotFoundError` se a origem
        não existir.
        """

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Se existe um objeto exatamente em `key`."""
