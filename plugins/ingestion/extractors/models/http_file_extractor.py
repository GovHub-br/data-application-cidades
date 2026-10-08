"""Estratégia `http_file`: baixa arquivos (xlsx, csv, zip...) como a fonte publica."""

from collections.abc import Iterator
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import requests

from ingestion.extractors.base_extractor import Extractor, RawFile
from ingestion.extractors.config_extractor import ExtractorConfig, HttpRequest
from ingestion.extractors.extractor_registry import ExtractorFactory
from ingestion.extractors.models.http_common import HttpHooks, fetch, save


@ExtractorFactory.register("http_file")
class HttpFileExtractor(Extractor):
    """Baixa cada arquivo de `config.requests` em stream, com o nome que a fonte deu.

    O nome vem do `Content-Disposition`; sem ele, do último segmento da URL final
    (depois de redirecionamentos); sem os dois, do `name` da chamada. Links que
    mudam a cada edição entram por `HttpRequest.resolve`; TLS fora do padrão, por
    `config.adapter`.
    """

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if not config.requests:
            raise ValueError("a estratégia http_file precisa de ao menos um arquivo")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        hooks = HttpHooks(self.config.conn_id, self.config.adapter)
        for request in self.config.requests:
            response = fetch(hooks, request)
            name = _file_name(response, request)
            yield save(response, work_dir / request.name / name, name)


def _file_name(response: requests.Response, request: HttpRequest) -> str:
    disposition = response.headers.get("Content-Disposition")
    if disposition:
        message = EmailMessage()
        message["Content-Disposition"] = disposition
        filename = message.get_filename()
        if filename:
            return PurePosixPath(filename).name
    # Corte explícito no último "/": uma URL terminada em barra não nomeia arquivo.
    from_url = unquote(urlparse(response.url).path).rsplit("/", 1)[-1]
    return from_url or request.name
