"""Estratégia `http_session`: executa os passos de `config.session` numa sessão."""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import requests
from airflow.sdk import Variable

from ingestion.extractors.base_extractor import Extractor, RawFile
from ingestion.extractors.config_extractor import ExtractorConfig
from ingestion.extractors.extractor_registry import ExtractorFactory
from ingestion.extractors.session.config import HttpSession
from ingestion.extractors.session.steps import Download, Run


def read_variable(name: str) -> str:
    """Variable do Airflow, lida em runtime (nunca no parse da DAG)."""
    return str(Variable.get(name))


@ExtractorFactory.register("http_session")
class HttpSessionExtractor(Extractor):
    """Uma `requests.Session` própria do começo ao fim do fluxo.

    O HttpHook abre sessão nova a cada chamada, e fluxos de login e formulário
    dependem de cookies e campos que passam de um passo ao seguinte. Cada
    `Download` do fluxo vira um arquivo da raw.
    """

    @classmethod
    def from_config(cls, config: ExtractorConfig, ingestion_time: datetime) -> Extractor:
        if not isinstance(config.session, HttpSession):
            raise ValueError("a estratégia http_session precisa de config.session")
        if not any(isinstance(step, Download) for step in config.session.steps):
            raise ValueError("o fluxo http_session precisa de ao menos um Download")
        return cls(config, ingestion_time)

    def extract(self, work_dir: Path) -> Iterator[RawFile]:
        flow: HttpSession = self.config.session
        work_dir.mkdir(parents=True, exist_ok=True)
        with requests.Session() as session:
            for prefix, adapter in flow.mounts.items():
                session.mount(prefix, adapter)
            context = {name: read_variable(var) for name, var in flow.variables.items()}
            run = Run(session=session, context=context, work_dir=work_dir)
            for step in flow.steps:
                produced = step.execute(run)
                if produced is not None:
                    yield produced
