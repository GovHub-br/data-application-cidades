"""Poupança SBPE mensal (ABECIP): depósitos, retiradas, captação, rendimento, saldo.

Fonte: planilha histórica da caderneta de poupança no site da ABECIP. O nome do
arquivo muda a cada edição (`cp-historico-<mês><ano>.xlsx`), então o link é
achado na página de indicadores (`link_in_page`) antes do download.

LoadMode: overwrite. Cada edição traz a série inteira desde 1982 e a ABECIP revisa
meses já publicados, então a última ingestão é a verdade.

Estrutura: a raw guarda a planilha inteira; a staging converte só a aba
`SBPE_Mensal` (o bronze em overwrite lê todo Parquet do `latest/`), com o
cabeçalho da linha 5. Totais anuais, meses futuros e rodapé ficam na staging e
saem na prata, onde um teste do dbt confere as identidades contábeis.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.extractors.resolvers import link_in_page
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

PAGE = "/credito-imobiliario/indicadores/caderneta-de-poupanca"
USER_AGENT = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

DATASET = DatasetSpec(
    domain="abecip",
    dataset="poupanca_sbpe_mensal",
    extractor=ExtractorConfig(
        source="http_file",
        base_url="https://www.abecip.org.br",
        requests=(
            HttpRequest(
                name="poupanca",
                endpoint=PAGE,
                headers=USER_AGENT,
                resolve=link_in_page(PAGE, "cp-historico", headers=USER_AGENT),
            ),
        ),
    ),
    converter=ConverterConfig(sheet="SBPE_Mensal", header_row=5),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="abecip_poupanca_trimestral_ingest_dag",
    # Diário às 06:00: a ABECIP publica uma vez por mês, sem data fixa.
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["abecip", "poupanca", "conjuntura", "ingestion"],
)
def abecip_poupanca_trimestral_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> list[str]:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefixes: list[str]) -> str:
        return steps.convert_to_staging(DATASET, raw_prefixes)

    convert_to_staging(extract_to_raw())


dag_instance = abecip_poupanca_trimestral_ingest_dag()
