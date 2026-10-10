"""Construtores de extrator: a estratégia escrita por extenso no DatasetSpec da DAG.

    extractor=api(base_url=ibge.SERVICODADOS, requests=ibge.agregado)

Cada um monta o `ExtractorConfig` da estratégia. Um argumento pode ser uma função da
entrada da Variable (a mecânica da fonte, no perfil): aí o construtor devolve uma
função da entrada, que o `DatasetSpec.resolve` chama dentro da task.
"""

from collections.abc import Callable, Mapping
from typing import Any

from ingestion.extractors.config_extractor import ExtractorConfig

#: Uma entrada da Variable da DAG (vazia quando a DAG tem um dataset só).
Entry = Mapping[str, Any]
ExtractorFromEntry = Callable[[Entry], ExtractorConfig]


def api(
    *, requests: Any, base_url: str | None = None, conn_id: str = "", adapter: Any = None
) -> ExtractorConfig | ExtractorFromEntry:
    """Chamadas JSON (`ApiExtractor`): uma parte por `HttpRequest`."""
    return _build(
        "api", requests=requests, base_url=base_url, conn_id=conn_id, adapter=adapter
    )


def http_file(
    *, requests: Any, base_url: str | None = None, conn_id: str = "", adapter: Any = None
) -> ExtractorConfig | ExtractorFromEntry:
    """Arquivo por URL (`HttpFileExtractor`), com `resolve` para link que muda."""
    return _build(
        "http_file",
        requests=requests,
        base_url=base_url,
        conn_id=conn_id,
        adapter=adapter,
    )


def http_session(*, flow: Any) -> ExtractorConfig | ExtractorFromEntry:
    """Fluxo numa sessão própria, como um navegador (`HttpSessionExtractor`)."""
    return _build("http_session", session=flow)


def email(*, mail: Any) -> ExtractorConfig | ExtractorFromEntry:
    """Anexos do e-mail do dia (`EmailAttachmentExtractor`, sobre o `ImapHook`)."""
    return _build("email", mail=mail)


def object_storage(*, objects: Any) -> ExtractorConfig | ExtractorFromEntry:
    """Objetos que outro processo gravou no bucket do lake."""
    return _build("object_storage", objects=objects)


def sftp(*, conn_id: str, remote: Any) -> ExtractorConfig | ExtractorFromEntry:
    """Arquivos de uma pasta remota (`SftpExtractor`, sobre o `SFTPHook`)."""
    return _build("sftp", conn_id=conn_id, remote=remote)


def _build(source: str, **fields: Any) -> ExtractorConfig | ExtractorFromEntry:
    if not any(callable(value) for value in fields.values()):
        return _config(source, fields)

    def from_entry(entry: Entry) -> ExtractorConfig:
        return _config(
            source,
            {
                name: value(entry) if callable(value) else value
                for name, value in fields.items()
            },
        )

    return from_entry


def _config(source: str, fields: dict[str, Any]) -> ExtractorConfig:
    if "requests" in fields:
        fields = {**fields, "requests": tuple(fields["requests"])}
    return ExtractorConfig(source=source, **fields)
