"""Estratégia `object_storage`: copia objetos que outro processo gravou no bucket."""

import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from ingestion.extractors.base_extractor import Extractor, RawFile, describe_file
from ingestion.extractors.config_extractor import ExtractorConfig, ObjectQuery
from ingestion.extractors.extractor_errors import ExtractionError
from ingestion.extractors.extractor_registry import ExtractorFactory
from ingestion.storage import storage_from_env


@ExtractorFactory.register("object_storage")
class ObjectStorageExtractor(Extractor):
    """Copia para a raw os objetos de `objects.prefix` que casam com o padrão.

    A fonte é o bucket do lake sem o prefixo de teste: o que outro time grava
    (`raw/abecip/<AAAA-MM>/...`, por exemplo) não muda com `INGESTION_STORAGE_PREFIX`.
    Um objeto por vez, em ordem de chave; nenhum objeto que case é erro (a
    extração de origem não depositou nada).
    """

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if config.objects is None:
            raise ValueError("a estratégia object_storage precisa de config.objects")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        query: ObjectQuery = self.config.objects  # type: ignore[assignment]
        source = storage_from_env(with_prefix=False)
        pattern = re.compile(query.pattern)
        keys = [key for key in source.list(query.prefix) if pattern.fullmatch(key)]
        if not keys:
            raise ExtractionError(
                f"nenhum objeto casa {query.pattern!r} em {query.prefix}"
            )
        for key in sorted(keys):
            name = (
                pattern.sub(query.rename, key) if query.rename else key.rsplit("/", 1)[-1]
            )
            path = work_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            source.get_file(key, path)
            yield describe_file(path, name)
