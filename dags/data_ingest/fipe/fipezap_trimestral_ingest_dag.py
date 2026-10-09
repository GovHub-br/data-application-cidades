"""Índice FipeZAP de locação residencial (FIPE).

Fonte: planilha pública de séries históricas da FIPE, num link estável
(`downloads.fipe.org.br`). Cada edição traz a série inteira desde 2008, com 59
abas (o índice consolidado e uma por cidade).

LoadMode: overwrite. A FIPE revisa a série retroativamente a cada divulgação,
então a última ingestão é a verdade.

Estrutura: a raw guarda a planilha inteira; a staging converte só a aba
`Índice FipeZAP` (o bronze em overwrite lê todo Parquet do `latest/`). Três
linhas de título mescladas e o cabeçalho na linha 4 (`Data`, `Total`, `Total`…,
que viram `Total_2`, `Total_3`…); `.` marca ausente. A prata escolhe as colunas
de locação, e um teste do dbt confere que índice e variações contam a mesma
história, porque a escolha é por posição.
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
    domain="fipe",
    dataset="indice_locacao",
    extractor=ExtractorConfig(
        source="http_file",
        conn_id="http_fipe",
        requests=(
            HttpRequest(
                name="fipezap",
                endpoint="/indices/fipezap/fipezap-serieshistoricas.xlsx",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            ),
        ),
    ),
    converter=ConverterConfig(sheet="Índice FipeZAP", header_row=4),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="fipezap_trimestral_ingest_dag",
    # Diário às 06:00: a FIPE divulga uma vez por mês, sem data fixa.
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fipezap", "locacao", "conjuntura", "ingestion"],
)
def fipezap_trimestral_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = fipezap_trimestral_ingest_dag()
