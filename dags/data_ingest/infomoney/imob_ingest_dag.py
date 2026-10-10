"""Índice IMOB (ações do setor imobiliário na B3), cotação diária.

Fonte: API do Alpha Vantage (`TIME_SERIES_DAILY`, `outputsize=compact`). O
símbolo e a chave vêm da Variable `api_key_alphavantage` (`acao`, `api_key`),
lida só dentro da task. A resposta traz a data do pregão como CHAVE de objeto
(`{"Time Series (Daily)": {"2026-10-08": {...}}}`); a conversão explode as
chaves em linhas (`key_column`).

LoadMode: merge por `data_pregao` (por símbolo: um arquivo por símbolo). A API
devolve só os últimos ~100 pregões, então o histórico se acumula pelas
ingestões. Antes, ele se acumulava no Postgres (`infomoney.acoes_imob`); o
histórico anterior à primeira ingestão nova entra por uma partição inicial,
gravada uma vez pelo script `scripts/ingestion/bootstrap_infomoney_imob.py`.

Limite de chamadas atingido: a API responde 200 com uma mensagem no lugar da
série; a conversão não acha `Time Series (Daily)` e falha, sem publicar nada.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task

from ingestion.converters import ConverterConfig
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps


def alpha_vantage() -> ExtractorConfig:
    """Uma chamada por símbolo da Variable; chamada só dentro da task."""
    config = Variable.get("api_key_alphavantage", deserialize_json=True)
    symbol = config["acao"]
    return ExtractorConfig(
        source="api",
        base_url="https://www.alphavantage.co",
        requests=(
            HttpRequest(
                name=symbol,
                endpoint="/query",
                params={
                    "function": "TIME_SERIES_DAILY",
                    "symbol": symbol,
                    "outputsize": "compact",
                    "apikey": config["api_key"],
                },
            ),
        ),
    )


DATASET = DatasetSpec(
    domain="infomoney",
    dataset="acoes_imob",
    extractor=alpha_vantage,
    converter=ConverterConfig(
        record_path="Time Series (Daily)", key_column="data_pregao"
    ),
    load_mode=LoadMode.MERGE,
    keys=("data_pregao",),
)


@dag(
    dag_id="infomoney_imob",
    # Diário às 06:00: cotação do pregão anterior.
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Milena Rocha",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["cidades", "infomoney", "imob", "cotações", "conjuntura", "ingestion"],
)
def infomoney_imob_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> list[str]:
        return steps.extract_to_raw(DATASET, context["dag_run"].run_after)

    @task
    def convert_to_staging(raw_prefixes: list[str]) -> str:
        return steps.convert_to_staging(DATASET, raw_prefixes)

    convert_to_staging(extract_to_raw())


dag_instance = infomoney_imob_dag()
