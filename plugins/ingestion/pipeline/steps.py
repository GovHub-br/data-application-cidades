"""Os passos que as DAGs encadeiam: extract_to_raw >> convert_to_staging.

O layout é o da main: cada arquivo pousa em `raw/<domain>/<nome>` por cima do
anterior, e vira `staging/<domain>/<nome sem extensão>.parquet`. Cada passo recebe
e devolve só chaves (XCom pequeno); o dado fica no storage. Storage e Connection
são resolvidos aqui, dentro da task, nunca no parse da DAG. Nada carrega no
Postgres: quem lê a staging é o pg_duckdb. Nada aqui conhece o Airflow: a DAG lê
as Variables e traduz o `NothingNew` em skip.
"""

import logging
import os
import posixpath
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from pathlib import Path

from ingestion.converters import ConverterFactory, Table, convert_table
from ingestion.dataset import DatasetSpec, IngestLog, Prepare
from ingestion.extractors import Entry, ExtractorFactory, RawFile, SourceNotFoundError
from ingestion.layout import raw_key, staging_key
from ingestion.storage import StorageBackend, storage_from_env


class NothingNew(Exception):
    """Nenhum dataset da execução trouxe dado novo (a DAG faz disso um skip)."""


def extract_to_raw(
    spec: DatasetSpec, run_after: datetime, entries: Sequence[Entry] | None = None
) -> dict[str, list[str]]:
    """Extrai cada dataset para `raw/<domain>/`; devolve as chaves gravadas, por dataset.

    `entries` é a lista da Variable da DAG: um dataset por entrada
    (`spec.resolve(entrada)`); sem ela, o spec é um dataset só. Os datasets rodam em
    sequência e o primeiro erro para a execução. Fonte sem o dado (404, e-mail do dia
    ausente) ou sem nada novo (extração incremental) não é erro: o dataset fica de
    fora do resultado. Se nenhum trouxer dado, `NothingNew`.

    Cada arquivo passa pelos `spec.prepare` antes do pouso (a raw nunca guarda PII).
    Com `spec.incremental`, o extrator recebe o que já pousou e cada arquivo pousado
    é registrado no controle logo depois do upload.
    """
    landed: dict[str, list[str]] = {}
    for dataset in _resolve_all(spec, entries):
        keys = _extract(dataset, run_after)
        if keys:
            landed[dataset.name] = keys
    if not landed:
        raise NothingNew(f"nenhum dado novo em {spec.domain}")
    return landed


def convert_to_staging(
    spec: DatasetSpec,
    raw: Mapping[str, Sequence[str]],
    entries: Sequence[Entry] | None = None,
) -> dict[str, list[str]]:
    """Converte os arquivos da raw de cada dataset e devolve, por dataset, os Parquets.

    `raw` é o que o `extract_to_raw` devolveu; `entries`, a mesma lista da Variable.
    Um arquivo por vez: baixa, converte com `spec.converter` e sobe cada Parquet
    (um por aba ou tabela) por cima do anterior.
    """
    storage = storage_from_env()
    staged: dict[str, list[str]] = {}
    for dataset in _resolve_all(spec, entries):
        keys = [
            key
            for source in raw.get(dataset.name, ())
            for key in _convert(storage, dataset, source)
        ]
        if keys:
            staged[dataset.name] = keys
    return staged


def _resolve_all(spec: DatasetSpec, entries: Sequence[Entry] | None) -> list[DatasetSpec]:
    if entries is None:
        return [spec.resolve()]
    if not entries:
        raise ValueError(f"a lista de datasets de {spec.domain} está vazia")
    datasets = [spec.resolve(entry) for entry in entries]
    names = [dataset.name for dataset in datasets]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(f"dataset repetido na lista de {spec.domain}: {repeated}")
    return datasets


def _extract(spec: DatasetSpec, run_after: datetime) -> list[str]:
    """As chaves gravadas de um dataset (vazio: a fonte não tinha o dado)."""
    storage = storage_from_env()
    extractor = ExtractorFactory.create(spec.extractor_config(), ingestion_time=run_after)
    if spec.incremental is not None:
        extractor.already_landed = spec.incremental.landed()
    keys: list[str] = []
    with tempfile.TemporaryDirectory(prefix="extract-", dir=_work_root()) as work:
        parts: Iterable[RawFile] = extractor.extract(Path(work))
        for step in spec.prepare:  # type: ignore[union-attr]
            parts = _apply(step, parts, Path(work))
        try:
            for part in parts:
                key = raw_key(spec.domain, part.name)
                if key in keys:
                    raise ValueError(f"nome repetido na ingestão: {part.name!r}")
                storage.put_file(key, part.path)
                part.path.unlink()
                keys.append(key)
                _record(spec.incremental, part, key)
        except SourceNotFoundError as exc:
            logging.warning("%s/%s: %s", spec.domain, spec.name, exc)
    if not keys:
        logging.info("%s/%s: nenhum arquivo novo", spec.domain, spec.name)
    return keys


def _record(log: IngestLog | None, part: RawFile, key: str) -> None:
    if log is not None:
        log.record(part, key)


def _convert(storage: StorageBackend, spec: DatasetSpec, source: str) -> list[str]:
    if not source.startswith(f"raw/{spec.domain}/"):
        raise ValueError(f"{source!r} não é da raw de {spec.domain}")
    with tempfile.TemporaryDirectory(prefix="convert-", dir=_work_root()) as work:
        local = Path(work) / "raw" / source.rsplit("/", 1)[-1]
        storage.get_file(source, local)
        if isinstance(spec.converter, Table):
            return _convert_table(storage, spec.converter, source, local, Path(work))
        keys: list[str] = []
        converter = ConverterFactory.for_file(local, spec.converter)
        for converted in converter.convert(local, Path(work) / "staging"):
            key = staging_key(source, converted.suffix)
            storage.put_file(key, converted.path)
            converted.path.unlink()
            keys.append(key)
        return keys


def _convert_table(
    storage: StorageBackend, table: Table, source: str, local: Path, work: Path
) -> list[str]:
    """A tabela da função da DAG na staging; o fallback, ao lado do arquivo na raw."""
    target = staging_key(source)
    current = None
    if table.accumulate and storage.exists(target):
        current = work / "current.parquet"
        storage.get_file(target, current)
    parquet, *fallback = convert_table(table, local, work / "staging", current=current)
    storage.put_file(target, parquet)
    for path in fallback:
        storage.put_file(f"{posixpath.dirname(source)}/fallback_json/{path.name}", path)
    return [target]


def _apply(step: Prepare, parts: Iterable[RawFile], work_dir: Path) -> Iterator[RawFile]:
    for part in parts:
        yield from step.apply(part, work_dir)


def _work_root() -> Path:
    """Disco de trabalho: LAKE_TMPDIR (volume do Airflow), não o /tmp em tmpfs."""
    root = Path(os.environ.get("LAKE_TMPDIR") or tempfile.gettempdir())
    root.mkdir(parents=True, exist_ok=True)
    return root
