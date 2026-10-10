"""Séries do SGS do BACEN (financiamento imobiliário e correlatas).

Fonte: API do SGS, uma chamada por série, com a série inteira
(`/dados/serie/bcdata.sgs.<código>/dados?formato=json`). As séries vêm da
Variable `BACEN_SERIES` ({tipo: código}), lida dentro da task de extração. Cada
série vira um arquivo `<tipo>.json` na raw e `<tipo>.parquet` na staging; a prata
tira o tipo do `filename`.

LoadMode: overwrite. O SGS devolve a série completa (o `ultimos=N` como parâmetro
de query é ignorado pela API), então a última ingestão é a verdade e revisões do
BACEN entram. Série diária exige janela (`dataInicial`) e responde 406 sem ela: a
extração falha com o endpoint na mensagem.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.dataset import DatasetSpec
from ingestion.extractors import HttpRequest, api
from ingestion.loaders import LoadMode
from ingestion.pipeline import NothingNew, steps


def series_sgs(series: Mapping[str, int] | list[Mapping[str, int]]) -> list[HttpRequest]:
    """Uma chamada por série de BACEN_SERIES ({tipo: código}), com a série inteira."""
    if isinstance(series, list):  # a Variable também aceita [{tipo: código}]
        series = series[0] if series else {}
    if not series:
        raise ValueError("a Variable BACEN_SERIES está vazia ou não existe")
    return [
        HttpRequest(
            name=tipo,
            endpoint=f"/dados/serie/bcdata.sgs.{codigo}/dados",
            params={"formato": "json"},
        )
        for tipo, codigo in series.items()
    ]


SGS = DatasetSpec(
    domain="bacen",
    dataset="financiamentos_imobiliarios",
    extractor=api(base_url="https://api.bcb.gov.br", requests=series_sgs),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="bacen_sgs_ingest_dag",
    # Diário às 06:00: a fonte é mensal, mas sem data fixa de publicação; rodar todo
    # dia deixa o latest/ fresco. A conjuntura_dag também dispara esta DAG na segunda.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Mateus", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bacen", "sgs", "financiamento_imobiliario", "conjuntura", "ingestion"],
)
def bacen_sgs_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> dict[str, list[str]]:
        series = Variable.get("BACEN_SERIES", deserialize_json=True)
        try:
            return steps.extract_to_raw(SGS, context["dag_run"].run_after, [series])
        except NothingNew as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, str]:
        return steps.convert_to_staging(SGS, raw)

    convert_to_staging(extract_to_raw())


dag_instance = bacen_sgs_ingest_dag()
