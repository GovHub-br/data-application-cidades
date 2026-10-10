"""Conversão de uma partição inteira da raw para a staging, e publicação do latest."""

import hashlib
import shutil
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

from ingestion.converters.base_converter import ConvertedFile
from ingestion.converters.config_converter import ConverterConfig
from ingestion.converters.converter_errors import ConversionError
from ingestion.converters.converter_registry import ConverterFactory
from ingestion.storage import SUCCESS_MARKER, StorageBackend, land, publish_latest
from ingestion.storage.landing import Part


@dataclass(frozen=True)
class StagedFile:
    """Parquet pronto para subir (satisfaz `storage.landing.Part`)."""

    name: str
    path: Path
    size: int
    sha256: str
    source: str
    rows: int
    columns: tuple[str, ...]
    skipped_rows: int = 0


@dataclass(frozen=True)
class ConversionResult:
    staging_prefix: str
    latest_prefix: str
    keys: tuple[str, ...]


def convert_partition(
    storage: StorageBackend,
    raw_prefix: str,
    staging_prefix: str,
    latest_prefix: str,
    config: ConverterConfig,
    work_dir: Path,
) -> ConversionResult:
    """Converte cada arquivo de uma ingestão completa da raw e publica o resultado.

    Exige o `_SUCCESS` da raw (ingestão incompleta não vira staging). Um arquivo da
    raw por vez: baixa, converte, sobe cada Parquet pelo `land` (que apaga a cópia
    local e recusa nome repetido entre arquivos) e apaga o arquivo baixado antes do
    próximo. O `_SUCCESS` da staging lista cada Parquet com a origem, as linhas e as
    colunas; só depois dele o `latest/` é trocado. Qualquer falha apaga o que já
    tinha subido para a partição (o glob do merge/append leria Parquet sem
    `_SUCCESS`) e deixa o `latest/` como estava.
    """
    if not storage.exists(raw_prefix + SUCCESS_MARKER):
        raise ConversionError(f"ingestão incompleta (sem {SUCCESS_MARKER}): {raw_prefix}")
    raw_keys = [
        key for key in storage.list(raw_prefix) if key != raw_prefix + SUCCESS_MARKER
    ]
    try:
        landed = land(
            storage,
            _staged_files(storage, raw_keys, config, work_dir),
            staging_prefix,
            details=_details,
        )
    except BaseException:
        for key in storage.list(staging_prefix):
            storage.delete(key)
        raise
    publish_latest(storage, staging_prefix, latest_prefix)
    return ConversionResult(
        staging_prefix=staging_prefix, latest_prefix=latest_prefix, keys=landed.keys
    )


def _staged_files(
    storage: StorageBackend, raw_keys: list[str], config: ConverterConfig, work_dir: Path
) -> Iterator[StagedFile]:
    for raw_key in raw_keys:
        local = work_dir / "raw" / raw_key.rsplit("/", 1)[-1]
        storage.get_file(raw_key, local)
        try:
            converter = ConverterFactory.for_file(local, config)
            for converted in converter.convert(local, work_dir / "staging"):
                yield _staged(converted, raw_key)
        finally:
            local.unlink(missing_ok=True)
    shutil.rmtree(work_dir / "raw", ignore_errors=True)


def _staged(converted: ConvertedFile, source: str) -> StagedFile:
    digest = hashlib.sha256()
    with converted.path.open("rb") as parquet:
        while chunk := parquet.read(1 << 20):
            digest.update(chunk)
    return StagedFile(
        name=converted.name,
        path=converted.path,
        size=converted.path.stat().st_size,
        sha256=digest.hexdigest(),
        source=source,
        rows=converted.rows,
        columns=converted.columns,
        skipped_rows=converted.skipped_rows,
    )


def _details(part: Part) -> Mapping[str, object]:
    assert isinstance(part, StagedFile)
    details: dict[str, object] = {
        "source": part.source,
        "rows": part.rows,
        "columns": list(part.columns),
    }
    if part.skipped_rows:
        details["skipped_rows"] = part.skipped_rows
    return details
