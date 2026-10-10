"""INCC-M (FGV), série histórica publicada pela Sinduscon-PR.

Fonte: xlsx público num link estável da Sinduscon, que redireciona para o arquivo
da edição corrente. A planilha traz a série inteira desde 1994 a cada edição.

LoadMode: overwrite. A fonte reentrega a série completa, então a última ingestão é
a verdade: revisões entram, e uma linha que a FGV tirar some do bronze.

Estrutura do arquivo: título na linha 1, cabeçalho em duas linhas (2 e 3), dados a
partir da 4, rodapé "Fonte: FGV" no fim. A staging lê o cabeçalho da linha 3 e
guarda tudo como texto; renomear e descartar o rodapé é da prata.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import http_file
from ingestion.loaders import LoadMode
from ingestion.pipeline import NothingNew, steps
from ingestion.sources import fgv

INCC_M = DatasetSpec(
    domain="fgv",
    dataset="incc_m",
    extractor=http_file(
        base_url=fgv.SINDUSCON_PR, 
        requests=(fgv.INCC_M,)
    ),
    converter=ConverterConfig(header_row=3),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="incc_m_ingest_dag",
    # Diário às 06:00: a fonte é mensal, mas sem data fixa de publicação; rodar todo
    # dia deixa o latest/ fresco. A conjuntura_dag também dispara esta DAG na segunda.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Gustavo", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fgv", "incc_m", "construcao", "custos", "conjuntura", "ingestion"],
)
def incc_m_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> dict[str, list[str]]:
        try:
            return steps.extract_to_raw(INCC_M, context["dag_run"].run_after)
        except NothingNew as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, str]:
        return steps.convert_to_staging(INCC_M, raw)

    convert_to_staging(extract_to_raw())


dag_instance = incc_m_ingest_dag()
