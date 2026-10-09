"""Estratégia `api`: uma resposta JSON por chamada, gravada como veio."""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from ingestion.extractors.base_extractor import Extractor, RawFile
from ingestion.extractors.config_extractor import ExtractorConfig
from ingestion.extractors.extractor_registry import ExtractorFactory
from ingestion.extractors.models.http_common import HttpHooks, fetch, save


@ExtractorFactory.register("api")
class ApiExtractor(Extractor):
    """Faz cada chamada de `config.requests` e grava o corpo em `<name>.json`.

    O corpo não passa por `json()`: a raw guarda o texto que a API mandou, e
    achatar ou tipar é trabalho do dbt. Uma resposta que declara Content-Type não
    JSON é repetida e, se persistir, falha a extração (ver `fetch`).

    Nenhuma fonte do cidades pagina hoje; quando uma paginar, a paginação entra aqui
    como mais um campo da configuração.
    """

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if not config.requests:
            raise ValueError(
                "a estratégia api precisa de ao menos uma chamada em requests"
            )
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        hooks = HttpHooks(self.config.conn_id, self.config.adapter)
        for request in self.config.requests:
            response = fetch(hooks, request, expect_json=True)
            yield save(
                response, work_dir / f"{request.name}.json", f"{request.name}.json"
            )
