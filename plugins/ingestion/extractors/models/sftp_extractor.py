"""Estratégia `sftp`: arquivos de uma pasta remota, escolhidos por padrão."""

import dataclasses
import logging
import posixpath
import re
import stat
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.providers.sftp.hooks.sftp import SFTPHook

from ingestion.extractors.base_extractor import Extractor, RawFile, write_stream
from ingestion.extractors.config_extractor import ExtractorConfig, RemoteFiles
from ingestion.extractors.extractor_errors import ExtractionError
from ingestion.extractors.extractor_registry import ExtractorFactory

#: Janela de leitura (`readv`): a memória fica nesse tamanho mesmo com arquivos de
#: dezenas de GB. Sem o prefetch do paramiko, que trava sob limitação de banda.
WINDOW_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class _Remote:
    path: str
    relative: str
    size: int
    mtime: int

    @property
    def name(self) -> str:
        return posixpath.basename(self.path)

    @property
    def source_id(self) -> str:
        # Sem a pasta: um arquivo movido para outra pasta (ANTERIORES/) não volta.
        return f"{self.name}:{self.size}:{self.mtime}"


@ExtractorFactory.register("sftp")
class SftpExtractor(Extractor):
    """Copia da pasta remota os arquivos que casam com `config.remote`, um por vez.

    Credencial na Connection (`conn_id`, do tipo SFTP/SSH). Incremental quando o
    `DatasetSpec` pede: `source_id` = nome + tamanho + data de modificação, e o que
    já pousou fica de fora. A mesma entrega em duas pastas (o mesmo `source_id`)
    vem uma vez só; nomes iguais com conteúdo diferente ficam com o mais recente.
    """

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if config.remote is None:
            raise ValueError("a estratégia sftp precisa de config.remote")
        if not config.conn_id:
            raise ValueError("a estratégia sftp precisa de conn_id")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        query: RemoteFiles = self.config.remote  # type: ignore[assignment]
        hook = SFTPHook(ssh_conn_id=self.config.conn_id)
        client = hook.get_conn()
        try:
            files = _select(list(_walk(client, query.root, "", query.recursive)), query)
            if not files:
                raise ExtractionError(
                    f"nenhum arquivo casa {query.pattern!r} em {query.root}"
                )
            work_dir.mkdir(parents=True, exist_ok=True)
            for remote in files:
                if remote.source_id in self.already_landed:
                    continue
                part = write_stream(
                    _download(client, remote), work_dir / remote.name, remote.name
                )
                yield dataclasses.replace(part, source_id=remote.source_id)
        finally:
            hook.close_conn()


def _walk(client: Any, root: str, relative: str, recursive: bool) -> Iterator[_Remote]:
    folder = posixpath.join(root, relative) if relative else root
    for entry in client.listdir_attr(folder):
        child = posixpath.join(relative, entry.filename) if relative else entry.filename
        if stat.S_ISDIR(entry.st_mode or 0):
            if recursive:
                yield from _walk(client, root, child, recursive)
            continue
        yield _Remote(
            path=posixpath.join(root, child),
            relative=child,
            size=int(entry.st_size or 0),
            mtime=int(entry.st_mtime or 0),
        )


def _select(files: list[_Remote], query: RemoteFiles) -> list[_Remote]:
    pattern = re.compile(query.pattern)
    excluded = [re.compile(rx) for rx in query.exclude]
    chosen = [
        f
        for f in files
        if pattern.search(f.relative)
        and not any(rx.search(f.relative) for rx in excluded)
    ]
    if query.prefer_extensions:
        chosen = _one_per_stem(chosen, query.prefer_extensions)
    by_name: dict[str, _Remote] = {}
    for remote in chosen:
        current = by_name.get(remote.name)
        if current is None or (remote.mtime, remote.size) > (current.mtime, current.size):
            if current is not None and current.source_id != remote.source_id:
                logging.warning(
                    "sftp: %s em duas pastas com conteúdo diferente; vale %s",
                    remote.name,
                    remote.relative,
                )
            by_name[remote.name] = remote
    return sorted(by_name.values(), key=lambda f: f.relative)


def _one_per_stem(files: list[_Remote], preference: tuple[str, ...]) -> list[_Remote]:
    """Uma entrega por identidade: o nome sem pasta e sem as extensões da lista.

    As extensões saem encadeadas (`X.TXT.zip` -> `X`), então `X.TXT`, `X.TXT.zip` e
    `X.zip`, em qualquer pasta, são a mesma entrega. Vence a extensão de fora mais
    à frente na lista; no empate, a mais nova.
    """
    rank = {ext.lower(): i for i, ext in enumerate(preference)}

    def order(remote: _Remote) -> tuple[int, int, int]:
        outer = posixpath.splitext(remote.name)[1].lower()
        return (rank.get(outer, len(rank)), -remote.mtime, -remote.size)

    best: dict[str, _Remote] = {}
    for remote in sorted(files, key=lambda f: f.relative):
        identity = _identity(remote.name, rank)
        current = best.get(identity)
        if current is None or order(remote) < order(current):
            best[identity] = remote
    return list(best.values())


def _identity(name: str, extensions: Mapping[str, int]) -> str:
    stem, ext = posixpath.splitext(name)
    while ext.lower() in extensions:
        name = stem
        stem, ext = posixpath.splitext(name)
    return name


def _download(client: Any, remote: _Remote) -> Iterator[bytes]:
    with client.open(remote.path, "rb") as source:
        for offset in range(0, remote.size, WINDOW_BYTES):
            size = min(WINDOW_BYTES, remote.size - offset)
            yield from source.readv([(offset, size)])
