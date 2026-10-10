"""PNAD Contínua, construção: ocupados e rendimento médio real (IBGE, SIDRA).

Fonte: API de valores da SIDRA (`apisidra.ibge.gov.br/values`), Brasil. Um dataset
por tabela da Variable `SIDRA_TABELAS` (valor canônico em
`infra/airflow/variables/SIDRA_TABELAS.json`), lida dentro das tasks: hoje 6323
(ocupados, variável 4090) e 6391 (rendimento, variável 5932), por grupamento de
atividade (classificação 888: 47946 = Total, 47949 = Construção), últimos 12
trimestres móveis.

    {"dataset": "pnad_construcao_ocupados", "tabela": 6323, "variavel": 4090,
     "periodos": 12, "classificacao": {"888": [47946, 47949]}}
 A resposta é uma lista JSON plana
cujo primeiro registro é o cabeçalho (`D3C` = "Trimestre Móvel (Código)"); a
staging guarda tudo e o cabeçalho sai na prata.

Reserva da API v3: o dbt lê o PNAD de `ibge_ingest_dag` (agregados 6323/6391
na v3); esta DAG fica para o caso de a v3 voltar a falhar para essas tabelas.

LoadMode: merge (`D3C`, `D4C`: trimestre e categoria). A SIDRA devolve só a
janela pedida, então o histórico se acumula pelas ingestões.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.dataset import DatasetSpec
from ingestion.extractors import HttpRequest, api
from ingestion.loaders import LoadMode
from ingestion.pipeline import NothingNew, steps

CAMPOS = {"dataset", "tabela", "variavel", "periodos", "classificacao"}


def tabela_sidra(entrada: Mapping[str, Any]) -> list[HttpRequest]:
    """A chamada da API de valores da SIDRA de uma tabela da Variable SIDRA_TABELAS."""
    desconhecidos = set(entrada) - CAMPOS
    faltando = CAMPOS - {"classificacao"} - set(entrada)
    if desconhecidos or faltando:
        raise ValueError(
            f"entrada de SIDRA_TABELAS: campo desconhecido {sorted(desconhecidos)}, "
            f"faltando {sorted(faltando)}"
        )
    endpoint = (
        f"/values/t/{entrada['tabela']}/n1/all/v/{entrada['variavel']}"
        f"/p/last%20{entrada['periodos']}"
    )
    for classificacao, categorias in entrada.get("classificacao", {}).items():
        lista = "all" if categorias == "all" else ",".join(map(str, categorias))
        endpoint += f"/c{classificacao}/{lista}"
    # sem User-Agent a SIDRA recusa a requisição
    return [
        HttpRequest(
            name=entrada["dataset"],
            endpoint=endpoint,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    ]


TABELA = DatasetSpec(
    domain="ibge",
    dataset=lambda entrada: entrada["dataset"],
    extractor=api(base_url="https://apisidra.ibge.gov.br", requests=tabela_sidra),
    load_mode=LoadMode.MERGE,
    keys=("D3C", "D4C"),
)


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
    @task
    def extract_to_raw(**context: Any) -> dict[str, list[str]]:
        tabelas = Variable.get("SIDRA_TABELAS", deserialize_json=True)
        try:
            return steps.extract_to_raw(TABELA, context["dag_run"].run_after, tabelas)
        except NothingNew as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, str]:
        tabelas = Variable.get("SIDRA_TABELAS", deserialize_json=True)
        return steps.convert_to_staging(TABELA, raw, tabelas)

    convert_to_staging(extract_to_raw())


dag_instance = ibge_pnad_construcao_sidra_ingest_dag()
