"""ABECIP: financiamentos SBPE por instituição financeira, uma série mensal.

O relatório mensal da ABECIP traz a abertura do financiamento imobiliário por
instituição, a tabela que o boletim publica na seção de crédito. Outro time
extrai o relatório (OCR do PDF) e grava uma competência por diretório em
`raw/abecip/<AAAA-MM>/financiamentos_por_instituicao.json`. Esta DAG não busca
nada na internet: copia essas competências (estratégia `object_storage`, do
bucket real, sem o prefixo de teste) para a raw do dataset, como
`<AAAA-MM>.json`, e a staging vira a série.

LoadMode: overwrite. Todas as competências a cada execução: a ABECIP revisa
competências já publicadas, e a última ingestão é a verdade. A competência sai
do nome do arquivo na prata (o campo interno já veio divergente do diretório).
Sem nenhuma competência no raw, a extração falha; série curta demais é acusada
pelo teste `conjuntura_abecip_instituicoes_historico` do dbt.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, ObjectQuery
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

DATASET = DatasetSpec(
    domain="abecip",
    dataset="financiamentos_por_instituicao",
    extractor=ExtractorConfig(
        source="object_storage",
        objects=ObjectQuery(
            prefix="raw/abecip/",
            pattern=r"raw/abecip/(\d{4}-\d{2})/financiamentos_por_instituicao\.json",
            rename=r"\1.json",
        ),
    ),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="abecip_instituicoes_ingest_dag",
    # Diário às 07:00: o outro time deposita a competência sem data fixa.
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["conjuntura", "abecip", "ingestao", "ingestion"],
)
def abecip_instituicoes_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = abecip_instituicoes_ingest_dag()
