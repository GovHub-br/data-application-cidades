"""INCC-M (FGV), série histórica publicada pela Sinduscon-PR.

Fonte: xlsx público num link estável da Sinduscon, que redireciona para o arquivo
da edição corrente. A planilha traz a série inteira desde 1994 a cada edição.

LoadMode: overwrite. A fonte reentrega a série completa, então a última ingestão é
a verdade: revisões entram, e uma linha que a FGV tirar some do bronze. O bronze é
`bronze_fgv_incc_m` (dbt, `select * from read_parquet` sobre o `latest/`).

Estrutura do arquivo: título na linha 1, cabeçalho em duas linhas (2 e 3), dados a
partir da 4, rodapé "Fonte: FGV" no fim. A staging lê o cabeçalho da linha 3
(`header_row=3`) e guarda tudo como texto; renomear e descartar o rodapé é da prata.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

DATASET = DatasetSpec(
    domain="fgv",
    dataset="incc_m",
    extractor=ExtractorConfig(
        source="http_file",
        conn_id="http_sinduscon",
        requests=(
            HttpRequest(
                name="incc_m",
                endpoint="/economia/indices-economicos/incc-m-fgv/"
                "9547-serie-historica-incc-m-fgv/",
                # Sem User-Agent de navegador o servidor recusa a requisição.
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            ),
        ),
    ),
    converter=ConverterConfig(header_row=3),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="incc_m_ingest_dag",
    # Provisório: o cron real vem da Variable dynamic_schedules na validação da Fase 5.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fgv", "incc_m", "construcao", "custos", "conjuntura", "ingestion"],
)
def incc_m_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = incc_m_ingest_dag()
