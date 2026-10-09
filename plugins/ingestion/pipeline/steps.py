"""Os passos que as DAGs encadeiam: extract_to_raw >> convert_to_staging.

Cada passo recebe e devolve só o prefixo de uma partição (XCom pequeno); o dado
fica no storage. Storage, Connection e Variable são resolvidos aqui, dentro da task,
nunca no parse da DAG. A carga no bronze é do dbt (`fonte_lake`).
"""

import json
import os
import tempfile
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path

from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import convert_partition
from ingestion.dataset import DatasetSpec, Prepare
from ingestion.extractors import ExtractorFactory, RawFile, SourceNotFoundError
from ingestion.layout import (
    ingestion_partition,
    latest_prefix,
    raw_prefix,
    staging_prefix,
)
from ingestion.storage import SUCCESS_MARKER, StorageBackend, land, storage_from_env


def extract_to_raw(spec: DatasetSpec, ingestion_time: datetime) -> str:
    """Extrai a fonte para `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/`.

    Devolve o prefixo da partição. Fonte sem o dado (404, e-mail do dia ausente) ou
    sem nenhum arquivo (nada novo, na extração incremental) vira skip da task.

    Com `spec.incremental`, o extrator recebe os `source_id`s dos manifestos das
    partições anteriores; cada arquivo passa pelos `spec.prepare` antes do pouso.
    """
    prefix = raw_prefix(spec.domain, spec.dataset, ingestion_partition(ingestion_time))
    storage = storage_from_env()
    extractor = ExtractorFactory.create(
        spec.extractor_config(), ingestion_time=ingestion_time
    )
    if spec.incremental:
        extractor.already_landed = _landed_source_ids(storage, spec)
    with tempfile.TemporaryDirectory(prefix="extract-", dir=_work_root()) as work:
        parts: Iterable[RawFile] = extractor.extract(Path(work))
        for step in spec.prepare:
            parts = _apply(step, parts, Path(work))
        try:
            landed = land(storage, parts, prefix, details=_details)
        except SourceNotFoundError as exc:
            raise AirflowSkipException(str(exc)) from exc
    if not landed.keys:
        raise AirflowSkipException(f"a fonte não entregou nenhum arquivo: {prefix}")
    return prefix


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
    base = f"raw/{spec.domain}/{spec.dataset}/"
    seen: set[str] = set()
    with tempfile.TemporaryDirectory(prefix="manifest-", dir=_work_root()) as work:
        for key in storage.list(base):
            if not key.endswith("/" + SUCCESS_MARKER):
                continue
            local = Path(work) / "m.json"
            storage.get_file(key, local)
            for entry in json.loads(local.read_text()).get("files", []):
                if entry.get("source_id"):
                    seen.add(str(entry["source_id"]))
    return frozenset(seen)


def convert_to_staging(spec: DatasetSpec, raw_prefix: str) -> str:
    """Converte a partição da raw para a staging e publica o `latest/`; devolve-o."""
    partition = _partition(spec, raw_prefix)
    latest = latest_prefix(spec.domain, spec.dataset)
    with tempfile.TemporaryDirectory(prefix="convert-", dir=_work_root()) as work:
        convert_partition(
            storage_from_env(),
            raw_prefix=raw_prefix,
            staging_prefix=staging_prefix(spec.domain, spec.dataset, partition),
            latest_prefix=latest,
            config=spec.converter,
            work_dir=Path(work),
        )
    return latest


def _partition(spec: DatasetSpec, prefix: str) -> str:
    base = f"raw/{spec.domain}/{spec.dataset}/"
    partition = prefix[len(base) :].rstrip("/") if prefix.startswith(base) else ""
    try:
        if raw_prefix(spec.domain, spec.dataset, partition) == prefix:
            return partition
    except ValueError:
        pass
    raise ValueError(
        f"{prefix!r} não é uma partição da raw de {spec.domain}/{spec.dataset}"
    )


def _work_root() -> Path:
    """Disco de trabalho: LAKE_TMPDIR (volume do Airflow), não o /tmp em tmpfs."""
    root = Path(os.environ.get("LAKE_TMPDIR") or tempfile.gettempdir())
    root.mkdir(parents=True, exist_ok=True)
    return root
