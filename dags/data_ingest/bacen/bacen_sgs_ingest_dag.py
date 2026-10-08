"""Séries do SGS do BACEN (financiamento imobiliário e correlatas).

Fonte: API do SGS, uma chamada por série, com a série inteira
(`/dados/serie/bcdata.sgs.<código>/dados?formato=json`). As séries vêm da Variable
`BACEN_SERIES` ({tipo: código}), lida só dentro da task. Cada série vira um arquivo
`<tipo>.json` na raw e `<tipo>.parquet` na staging; a prata tira o tipo do
`filename`.

LoadMode: overwrite. O SGS devolve a série completa (o `ultimos=N` como parâmetro
de query é ignorado pela API), então a última ingestão é a verdade e revisões do
BACEN entram. Série diária exige janela (`dataInicial`) e responde 406 sem ela: a
extração falha com o endpoint na mensagem.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task

from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps


def bacen_series() -> ExtractorConfig:
    """Uma chamada por série de BACEN_SERIES; chamada só dentro da task."""
    series = Variable.get("BACEN_SERIES", deserialize_json=True, default={})
    if isinstance(series, list):  # a Variable também aceita [{tipo: código}]
        series = series[0] if series else {}
    if not series:
        raise ValueError("a Variable BACEN_SERIES está vazia ou não existe")
    return ExtractorConfig(
        source="api",
        conn_id="http_bacen",
        requests=tuple(
            HttpRequest(
                name=tipo,
                endpoint=f"/dados/serie/bcdata.sgs.{codigo}/dados",
                params={"formato": "json"},
            )
            for tipo, codigo in series.items()
        ),
    )


DATASET = DatasetSpec(
    domain="bacen",
    dataset="financiamentos_imobiliarios",
    extractor=bacen_series,
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="bacen_sgs_ingest_dag",
    # Provisório: o cron real vem da Variable dynamic_schedules na validação da Fase 5.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bacen", "sgs", "financiamento_imobiliario", "conjuntura", "ingestion"],
)
def bacen_sgs_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = bacen_sgs_ingest_dag()
