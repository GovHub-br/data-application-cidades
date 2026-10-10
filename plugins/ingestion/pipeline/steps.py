"""Os passos que as DAGs encadeiam: extract_to_raw >> convert_to_staging.

Cada passo recebe e devolve só prefixos de partição (XCom pequeno); o dado fica
no storage. Storage e Connection são resolvidos aqui, dentro da task, nunca no
parse da DAG. A carga no bronze é do dbt (`fonte_lake`). Nada aqui conhece o
Airflow: a DAG lê as Variables e traduz o `NothingNew` em skip.
"""

import json
import logging
import os
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path

from ingestion.converters import convert_partition
from ingestion.dataset import DatasetSpec, Prepare
from ingestion.extractors import Entry, ExtractorFactory, RawFile, SourceNotFoundError
from ingestion.layout import (
    ingestion_partition,
    latest_prefix,
    raw_prefix,
    staging_prefix,
)
from ingestion.storage import SUCCESS_MARKER, StorageBackend, land, storage_from_env


class NothingNew(Exception):
    """Nenhum dataset da execução trouxe dado novo (a DAG faz disso um skip)."""


def extract_to_raw(
    spec: DatasetSpec, run_after: datetime, entries: Sequence[Entry] | None = None
) -> dict[str, list[str]]:
    """Extrai cada dataset para `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/`.

    `entries` é a lista da Variable da DAG: um dataset por entrada
    (`spec.resolve(entrada)`); sem ela, o spec é um dataset só. Devolve, por dataset,
    os prefixos das partições gravadas, em ordem. Os datasets rodam em sequência e
    o primeiro erro para a execução. Fonte sem o dado (404, e-mail do dia ausente)
    ou sem nada novo (extração incremental) não é erro: o dataset fica de fora do
    resultado. Se nenhum trouxer dado, `NothingNew`.

    Sem `spec.incremental`, a execução é uma ingestão: uma partição, com a data da
    execução (`run_after`). Com `spec.incremental`, o extrator recebe os
    `source_id`s dos manifestos anteriores, e cada entrega nova vira uma ingestão
    própria, na ordem em que o extrator as entrega (a de chegada na fonte), com a
    data do pouso: na primeira carga, o histórico entra como uma sequência de
    ingestões, e a última é a entrega mais recente.

    Cada arquivo passa pelos `spec.prepare` antes do pouso. O manifesto guarda em
    `sources` as origens que a ingestão consumiu, inclusive a que os preparos
    descartaram inteira (o pacote sem a família procurada, registrado na ingestão
    seguinte), para que ela não volte. O mesmo nome duas vezes numa ingestão fica
    com o primeiro, e os outros vão para `duplicates`.
    """
    landed: dict[str, list[str]] = {}
    for dataset in _resolve_all(spec, entries):
        prefixes = _extract(dataset, run_after)
        if prefixes:
            landed[dataset.name] = prefixes
    if not landed:
        raise NothingNew(f"nenhum dado novo em {spec.domain}")
    return landed


def convert_to_staging(
    spec: DatasetSpec,
    raw: Mapping[str, Sequence[str]],
    entries: Sequence[Entry] | None = None,
) -> dict[str, str]:
    """Converte as partições da raw de cada dataset e devolve, por dataset, o `latest/`.

    `raw` é o que o `extract_to_raw` devolveu; `entries`, a mesma lista da Variable.
    Cada partição convertida é publicada no `latest/`, em ordem: ao fim, ele tem a
    última, e o manifesto dele aponta a anterior como antecessora. Com
    `spec.incremental`, também converte as ingestões que ficaram na raw sem staging
    (a execução caiu entre o pouso e a conversão, e a extração seguinte não as baixa
    de novo).
    """
    published: dict[str, str] = {}
    storage = storage_from_env()
    for dataset in _resolve_all(spec, entries):
        prefixes = list(raw.get(dataset.name, ()))
        if dataset.incremental:
            prefixes = sorted(set(prefixes) | set(_without_staging(storage, dataset)))
        if prefixes:
            published[dataset.name] = _convert(storage, dataset, prefixes)
    return published


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
    """As partições gravadas de um dataset (vazio: a fonte não tinha o dado)."""
    storage = storage_from_env()
    extractor = ExtractorFactory.create(spec.extractor_config(), ingestion_time=run_after)
    if spec.incremental:
        extractor.already_landed = _landed_source_ids(storage, spec)
    sources: list[str] = []
    prefixes: list[str] = []
    with tempfile.TemporaryDirectory(prefix="extract-", dir=_work_root()) as work:
        parts: Iterable[RawFile] = _track(extractor.extract(Path(work)), sources)
        for step in spec.prepare:  # type: ignore[union-attr]
            parts = _apply(step, parts, Path(work))
        try:
            if spec.incremental:
                _land_each_delivery(storage, spec, parts, sources, prefixes)
            else:
                prefix = raw_prefix(
                    spec.domain, spec.name, ingestion_partition(run_after)
                )
                if _land(storage, spec, parts, prefix, lambda: sources):
                    prefixes.append(prefix)
        except SourceNotFoundError as exc:
            logging.warning("%s/%s: %s", spec.domain, spec.name, exc)
    if not prefixes:
        logging.info("%s/%s: nenhum arquivo novo", spec.domain, spec.name)
    return prefixes


def _convert(storage: StorageBackend, spec: DatasetSpec, prefixes: Sequence[str]) -> str:
    latest = latest_prefix(spec.domain, spec.name)
    for prefix in prefixes:
        partition = _partition(spec, prefix)
        with tempfile.TemporaryDirectory(prefix="convert-", dir=_work_root()) as work:
            convert_partition(
                storage,
                raw_prefix=prefix,
                staging_prefix=staging_prefix(spec.domain, spec.name, partition),
                latest_prefix=latest,
                config=spec.converter,
                work_dir=Path(work),
            )
    return latest


def _land_each_delivery(
    storage: StorageBackend,
    spec: DatasetSpec,
    parts: Iterable[RawFile],
    sources: list[str],
    prefixes: list[str],
) -> None:
    """Uma partição por entrega (`source_id`), na ordem, com a data do pouso."""
    recorded: set[str] = set()
    previous: datetime | None = None
    for source_id, delivery in groupby(parts, key=lambda part: part.source_id):
        previous = _landing_time(previous)
        prefix = raw_prefix(spec.domain, spec.name, ingestion_partition(previous))
        while storage.list(prefix):  # outra execução no mesmo segundo
            previous += timedelta(seconds=1)
            prefix = raw_prefix(spec.domain, spec.name, ingestion_partition(previous))
        # o groupby já leu o 1º arquivo da entrega seguinte: só até esta conta
        upto = sources.index(source_id) + 1 if source_id in sources else len(sources)
        consumed = [s for s in sources[:upto] if s not in recorded]
        if _land(storage, spec, delivery, prefix, lambda: consumed):
            recorded.update(consumed)
            prefixes.append(prefix)


def _landing_time(previous: datetime | None) -> datetime:
    """Agora, ou um segundo depois da ingestão anterior desta execução."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if previous is not None and now <= previous:
        return previous + timedelta(seconds=1)
    return now


def _land(
    storage: StorageBackend,
    spec: DatasetSpec,
    parts: Iterable[RawFile],
    prefix: str,
    consumed: Callable[[], list[str]],
) -> bool:
    duplicates: list[dict[str, object]] = []
    if spec.prepare:
        parts = _distinct(parts, duplicates)
    landed = land(
        storage,
        parts,
        prefix,
        details=_details,
        summary=lambda: {
            "sources": sorted(consumed()),
            **({"duplicates": duplicates} if duplicates else {}),
        },
    )
    return bool(landed.keys)


def _track(parts: Iterable[RawFile], sources: list[str]) -> Iterator[RawFile]:
    for part in parts:
        if part.source_id and part.source_id not in sources:
            sources.append(part.source_id)
        yield part


def _distinct(
    parts: Iterable[RawFile], duplicates: list[dict[str, object]]
) -> Iterator[RawFile]:
    names: set[str] = set()
    for part in parts:
        if part.name in names:
            logging.warning(
                "%s repetido (origem %s): fica o primeiro", part.name, part.source_id
            )
            duplicates.append({"name": part.name, "source_id": part.source_id})
            part.path.unlink(missing_ok=True)
            continue
        names.add(part.name)
        yield part


def _apply(step: Prepare, parts: Iterable[RawFile], work_dir: Path) -> Iterator[RawFile]:
    for part in parts:
        yield from step.apply(part, work_dir)


def _details(part: object) -> dict[str, object]:
    """O que o manifesto guarda além de nome, tamanho e sha256."""
    extra: dict[str, object] = dict(getattr(part, "details", {}) or {})
    source_id = getattr(part, "source_id", None)
    if source_id:
        extra["source_id"] = source_id
    return extra


def _landed_source_ids(storage: StorageBackend, spec: DatasetSpec) -> frozenset[str]:
    """`source_id`s de todas as partições completas da raw do dataset."""
    base = f"raw/{spec.domain}/{spec.name}/"
    seen: set[str] = set()
    with tempfile.TemporaryDirectory(prefix="manifest-", dir=_work_root()) as work:
        for key in storage.list(base):
            if not key.endswith("/" + SUCCESS_MARKER):
                continue
            local = Path(work) / "m.json"
            storage.get_file(key, local)
            manifest = json.loads(local.read_text())
            seen.update(str(source) for source in manifest.get("sources", []))
            for entry in manifest.get("files", []):
                if entry.get("source_id"):
                    seen.add(str(entry["source_id"]))
    return frozenset(seen)


def _without_staging(storage: StorageBackend, spec: DatasetSpec) -> list[str]:
    """Partições completas da raw cuja staging não tem `_SUCCESS`."""
    base = f"raw/{spec.domain}/{spec.name}/"
    pending = []
    for key in storage.list(base):
        if not key.endswith("/" + SUCCESS_MARKER):
            continue
        prefix = key[: -len(SUCCESS_MARKER)]
        staged = staging_prefix(spec.domain, spec.name, _partition(spec, prefix))
        if not storage.exists(staged + SUCCESS_MARKER):
            pending.append(prefix)
    return pending


def _partition(spec: DatasetSpec, prefix: str) -> str:
    base = f"raw/{spec.domain}/{spec.name}/"
    partition = prefix[len(base) :].rstrip("/") if prefix.startswith(base) else ""
    try:
        if raw_prefix(spec.domain, spec.name, partition) == prefix:
            return partition
    except ValueError:
        pass
    raise ValueError(f"{prefix!r} não é uma partição da raw de {spec.domain}/{spec.name}")


def _work_root() -> Path:
    """Disco de trabalho: LAKE_TMPDIR (volume do Airflow), não o /tmp em tmpfs."""
    root = Path(os.environ.get("LAKE_TMPDIR") or tempfile.gettempdir())
    root.mkdir(parents=True, exist_ok=True)
    return root
