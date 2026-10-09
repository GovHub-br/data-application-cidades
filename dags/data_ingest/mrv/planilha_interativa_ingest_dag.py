"""MRV: Planilha Interativa da Central de Resultados (dados operacionais).

Fonte: o site de RI da MRV hospeda os documentos no catálogo da MZ (mziq). A
planilha do trimestre mais recente é achada na listagem JSON do catálogo
(`latest_in_json_listing`: categoria da Planilha Interativa, maior trimestre, no
ano corrente ou, sem documento ainda, nos dois anteriores) e baixada do CDN com o nome que a MZ dá
(`mrve3_base_de_dados_operacionais_e_financeiros.xlsx`).

Substitui as antigas `lancamentos_ingest_dag` e `vendas_ingest_dag`, que baixavam
a mesma planilha e recortavam blocos diferentes da mesma aba. Nenhum modelo do dbt
lê a MRV hoje (o boletim usa o dado manual de balanços); o dado vai só até a
staging, e lançamentos e vendas viram recorte na prata quando alguém precisar.

LoadMode: overwrite. Cada edição traz todos os trimestres em colunas.

Estrutura: a staging converte só a aba de dados operacionais, achada por
expressão (`^Dados Oper.`), porque o sufixo muda entre edições; o cabeçalho com
os trimestres (`3T26 / 3Q26`…) está na linha 2.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.extractors.resolvers import latest_in_json_listing
from ingestion.layout import TIMEZONE
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

MRV_COMPANY_ID = "4b56353d-d5d9-435f-bf63-dcbf0a6c25d5"
CATALOGO = f"/filemanager/company/{MRV_COMPANY_ID}/filter/categories/year/meta"
CATEGORIA = "central_de_resultados_planilha_interativa"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Origin": "https://ri.mrv.com.br",
    "Referer": "https://ri.mrv.com.br/",
}

def catalogo() -> ExtractorConfig:
    """A planilha mais recente do catálogo; os anos tentados dependem da data."""
    this_year = datetime.now(TIMEZONE).year
    return ExtractorConfig(
        source="http_file",
        base_url="https://apicatalog.mziq.com",
        requests=(
            HttpRequest(
                name="planilha_interativa",
                endpoint=CATALOGO,
                headers=HEADERS,
                resolve=latest_in_json_listing(
                    CATALOGO,
                    method="POST",
                    json={
                        "categories": [CATEGORIA],
                        "language": "pt_BR",
                        "published": True,
                    },
                    headers=HEADERS,
                    items="data.document_metas",
                    where={"internal_name": CATEGORIA},
                    order_by="file_quarter",
                    pick="permalink",
                    attempts=tuple(
                        {"year": str(year)}
                        for year in range(this_year, this_year - 3, -1)
                    ),
                ),
            ),
        ),
    )


DATASET = DatasetSpec(
    domain="mrv",
    dataset="planilha_interativa",
    extractor=catalogo,
    converter=ConverterConfig(include=r"^Dados Oper\.", header_row=2),
    load_mode=LoadMode.OVERWRITE,
)


@dag(
    dag_id="mrv_planilha_interativa_ingest_dag",
    # Diário às 06:00: a MRV publica uma vez por trimestre, sem data fixa.
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mrv", "lancamentos", "vendas", "operacionais", "ingestion"],
)
def mrv_planilha_interativa_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(DATASET, raw_prefix)

    convert_to_staging(extract_to_raw())


dag_instance = mrv_planilha_interativa_ingest_dag()
