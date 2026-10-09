"""ICST (FGV-IBRE): Índice de Confiança da Construção, com e sem ajuste sazonal.

Fonte: portal FGVDados, sem API. O CSV da série histórica só sai depois do login
no OutSystems e de navegar no FGVDados (ASP.NET); a estratégia `fgvdados` faz o
caminho numa sessão só. As credenciais vêm das Variables `dados_fgv_email` e
`dados_fgv_password`, lidas só dentro da task.

LoadMode: overwrite. O CSV traz a série inteira desde 07/2010 a cada download,
então a última ingestão é a verdade e revisões da FGV entram.

Estrutura do arquivo: latin-1, `;`, cabeçalho na linha 1 com o nome longo de
cada série e o código da FGV entre parênteses; valores com vírgula decimal.
Renomear e tipar é da prata (`prata_conjuntura_fgv_icst`).
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, FgvDadosQuery
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

DATASET = DatasetSpec(
    domain="fgv",
    dataset="icst",
    extractor=ExtractorConfig(
        source="fgvdados",
        fgvdados=FgvDadosQuery(
            series="ICST",
            email_variable="dados_fgv_email",
            password_variable="dados_fgv_password",
        ),
    ),
    converter=ConverterConfig(encoding="latin-1", delimiter=";"),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="icst_ingest_dag",
    # Diário às 06:00: a FGV publica uma vez por mês, sem data fixa.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fgv", "icst", "construcao", "confianca", "conjuntura", "ingestion"],
)
def icst_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = icst_ingest_dag()
