"""PNAD Contínua, construção: ocupados e rendimento médio real (IBGE, SIDRA).

Fonte: API de valores da SIDRA (`apisidra.ibge.gov.br/values`), tabelas 6323
(ocupados, variável 4090) e 6391 (rendimento, variável 5932), Brasil, por
grupamento de atividade (classificação 888: 47946 = Total, 47949 =
Construção), últimos 12 trimestres móveis. A resposta é uma lista JSON plana
cujo primeiro registro é o cabeçalho (`D3C` = "Trimestre Móvel (Código)"); a
staging guarda tudo e o cabeçalho sai na prata.

Reserva da API v3: o dbt lê o PNAD de `ibge_ingest_dag` (agregados 6323/6391
na v3); esta DAG fica para o caso de a v3 voltar a falhar para essas tabelas.

LoadMode: merge (`D3C`, `D4C`: trimestre e categoria). A SIDRA devolve só a
janela pedida, então o histórico se acumula pelas ingestões.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import TaskGroup, dag, task

from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

# Classificação 888 (grupamento de atividade): 47946 = Total, 47949 = Construção.
CLASSIFICACAO = "c888/47946,47949"
TABELAS = {
    "pnad_construcao_ocupados": (6323, 4090),
    "pnad_construcao_rendimento": (6391, 5932),
}

DATASETS = tuple(
    DatasetSpec(
        domain="ibge",
        dataset=dataset,
        extractor=ExtractorConfig(
            source="api",
            base_url="https://apisidra.ibge.gov.br",
            requests=(
                HttpRequest(
                    name=dataset,
                    endpoint=(
                        f"/values/t/{tabela}/n1/all/v/{variavel}/p/last%2012/"
                        f"{CLASSIFICACAO}"
                    ),
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
            ),
        ),
        load_mode=LoadMode.MERGE,
        keys=("D3C", "D4C"),
    )
    for dataset, (tabela, variavel) in TABELAS.items()
)


def _pipeline(spec: DatasetSpec) -> None:
    @task(task_id="extract_to_raw")
    def extract_to_raw(**context: Any) -> list[str]:
        return steps.extract_to_raw(spec, context["dag_run"].run_after)

    @task(task_id="convert_to_staging")
    def convert_to_staging(raw_prefixes: list[str]) -> str:
        return steps.convert_to_staging(spec, raw_prefixes)

    convert_to_staging(extract_to_raw())


@dag(
    dag_id="ibge_pnad_construcao_sidra_ingest_dag",
    # Diário às 06:00: a PNAD-C sai por trimestre móvel, uma vez por mês.
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ibge", "pnad", "construcao", "sidra", "conjuntura", "ingestion"],
)
def ibge_pnad_construcao_sidra_ingest_dag() -> None:
    for spec in DATASETS:
        with TaskGroup(group_id=spec.dataset):
            _pipeline(spec)


dag_instance = ibge_pnad_construcao_sidra_ingest_dag()
