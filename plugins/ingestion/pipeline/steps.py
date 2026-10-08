"""Os passos que as DAGs encadeiam: extract_to_raw >> convert_to_staging.

Cada passo recebe e devolve só o prefixo de uma partição (XCom pequeno); o dado
fica no storage. Storage, Connection e Variable são resolvidos aqui, dentro da task,
nunca no parse da DAG. A carga no bronze é do dbt (`fonte_lake`).
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path

from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import convert_partition
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorFactory, SourceNotFoundError
from ingestion.layout import (
    ingestion_partition,
    latest_prefix,
    raw_prefix,
    staging_prefix,
)
from ingestion.storage import land, storage_from_env


def extract_to_raw(spec: DatasetSpec, ingestion_time: datetime) -> str:
    """Extrai a fonte para `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/`.

    Devolve o prefixo da partição. Fonte sem o dado (404, e-mail do dia ausente) ou
    sem nenhum arquivo vira skip da task, não falha.
    """
    prefix = raw_prefix(spec.domain, spec.dataset, ingestion_partition(ingestion_time))
    extractor = ExtractorFactory.create(
        spec.extractor_config(), ingestion_time=ingestion_time
    )
    with tempfile.TemporaryDirectory(prefix="extract-", dir=_work_root()) as work:
        try:
            landed = land(storage_from_env(), extractor.extract(Path(work)), prefix)
        except SourceNotFoundError as exc:
            raise AirflowSkipException(str(exc)) from exc
    if not landed.keys:
        raise AirflowSkipException(f"a fonte não entregou nenhum arquivo: {prefix}")
    return prefix


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
