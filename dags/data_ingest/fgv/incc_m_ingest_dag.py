"""INCC-M (FGV), série histórica publicada pela Sinduscon-PR.

Fonte: xlsx público num link estável da Sinduscon, que redireciona para o arquivo
da edição corrente. A planilha traz a série inteira desde 1994 a cada edição.

Raw: `raw/fgv/incc_m.xlsx`, sobrescrito a cada execução, mais os registros em
`raw/fgv/fallback_json/incc_m.json`. Staging: `staging/fgv/incc_m.parquet`, só
texto, com as colunas de sempre (`mes`, `indice`, `var_mes`, `var_ano`,
`var_12_meses`, `dt_ingest`).
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import Table
from ingestion.dataset import DatasetSpec
from ingestion.extractors import HttpRequest, http_file
from ingestion.pipeline import NothingNew, steps


def incc_m(raw: Path) -> pd.DataFrame:
    """A série da planilha: título e cabeçalho em duas linhas no topo (pula 3),
    colunas A a E, rodapé "Fonte: FGV" e linhas vazias fora."""
    df = pd.read_excel(
        raw,
        skiprows=3,
        usecols="A:E",
        names=["mes", "indice", "var_mes", "var_ano", "var_12_meses"],
    )
    df = df.dropna(subset=["mes"])
    df = df[~df["mes"].astype(str).str.contains("Fonte", case=False, na=False)]
    df["dt_ingest"] = datetime.now().isoformat()
    return df.where(pd.notnull(df), None)


INCC_M = DatasetSpec(
    domain="fgv",
    dataset="incc_m",
    extractor=http_file(
        base_url="https://sindusconpr.com.br",
        requests=(
            HttpRequest(
                name="incc_m",
                # link estável: redireciona para o xlsx da edição corrente
                endpoint=(
                    "/economia/indices-economicos/incc-m-fgv/"
                    "9547-serie-historica-incc-m-fgv/"
                ),
                # sem User-Agent de navegador o servidor recusa a requisição
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                filename="incc_m.xlsx",
            ),
        ),
    ),
    converter=Table(incc_m, fallback_json=True),
)


@dag(
    dag_id="incc_m_ingest_dag",
    # Diário às 06:00: a fonte é mensal, mas sem data fixa de publicação; rodar todo
    # dia deixa a staging fresca. A conjuntura_dag também dispara esta DAG na segunda.
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
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, list[str]]:
        return steps.convert_to_staging(INCC_M, raw)

    convert_to_staging(extract_to_raw())


dag_instance = incc_m_ingest_dag()
