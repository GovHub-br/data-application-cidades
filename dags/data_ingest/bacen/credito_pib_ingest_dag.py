"""Crédito imobiliário / PIB (%), mensal (BACEN, Olinda MercadoImobiliario).

Fonte: API OData do Olinda, recurso `mercadoimobiliario`, filtrado pelo indicador
`indices_imobiliario_pib_br`. O indicador não está no SGS. A resposta é JSON
(`{"value": [{"Data", "Info", "Valor"}]}`), gravada como veio.

O filtro vai no próprio endpoint, já com `%20`: passado como parâmetro, o espaço
vira `+` e o Olinda responde HTTP 400.

LoadMode: overwrite. A consulta devolve a série inteira (desde 2014-04), então a
última ingestão é a verdade.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import HttpRequest, api
from ingestion.loaders import LoadMode
from ingestion.pipeline import NothingNew, steps

RESOURCE = "/olinda/servico/MercadoImobiliario/versao/v1/odata/mercadoimobiliario"

CREDITO_PIB = DatasetSpec(
    domain="bacen",
    dataset="credito_imobiliario_pib",
    extractor=api(
        base_url="https://olinda.bcb.gov.br",
        requests=(
            HttpRequest(
                name="credito_imobiliario_pib",
                endpoint=(
                    f"{RESOURCE}?$filter=Info%20eq%20'indices_imobiliario_pib_br'"
                    "&$orderby=Data&$format=json"
                ),
            ),
        ),
    ),
    converter=ConverterConfig(record_path="value.item"),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="bacen_credito_pib_ingest_dag",
    # Diário às 06:00: o BACEN publica uma vez por mês, sem data fixa.
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bacen", "imobiliario", "credito_pib", "conjuntura", "ingestion"],
)
def bacen_credito_pib_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> dict[str, list[str]]:
        try:
            return steps.extract_to_raw(CREDITO_PIB, context["dag_run"].run_after)
        except NothingNew as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, list[str]]:
        return steps.convert_to_staging(CREDITO_PIB, raw)

    convert_to_staging(extract_to_raw())


dag_instance = bacen_credito_pib_ingest_dag()
