"""IBGE, API v3 de agregados: PIB, SINAPI, PAIC, PNAD-C, PIM-PF e PMC.

Fonte: `servicodados.ibge.gov.br/api/v3/agregados/<agregado>/periodos/<janela>/
variaveis/<v1|v2...>`, Brasil (`N1[1]`). Um dataset por agregado da Variable
`IBGE_AGREGADOS` (valor canônico em `infra/airflow/variables/IBGE_AGREGADOS.json`),
lida dentro das tasks:

    {"dataset": "pib_construcao", "agregado": 5932,
     "variaveis": [6564, 6563, 6562, 6561], "periodos": 20,
     "classificacao": {"11255": [90694]}}

`periodos` são os últimos N; `classificacao` é opcional, com uma classificação só
(a API recusa duas na mesma chamada, com HTTP 500) e `"all"` para todas as
categorias.

LoadMode: merge pelas chaves do achatamento. Cada chamada traz só a janela pedida,
então o histórico se acumula pelas ingestões, e revisões do IBGE dentro da janela
substituem o valor anterior.

Conversão: uma linha por (variável, localidade, classificação, categoria,
período), em texto. A resposta da v3 tem, em cada variável, `resultados` (uma
combinação de categorias) e neles uma `serie` por localidade, com o PERÍODO COMO
CHAVE. Sem classificação, ids `"0"` e nomes vazios. A tipagem é da prata (macro
`ibge_v3_tipado`).
"""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException

from ingestion.converters import ConverterConfig, Field
from ingestion.dataset import DatasetSpec
from ingestion.extractors import HttpRequest, api
from ingestion.loaders import LoadMode
from ingestion.pipeline import NothingNew, steps

CAMPOS = {"dataset", "agregado", "variaveis", "periodos", "classificacao"}


def agregado(entrada: Mapping[str, Any]) -> list[HttpRequest]:
    """A chamada da API v3 de um agregado da Variable IBGE_AGREGADOS."""
    desconhecidos = set(entrada) - CAMPOS
    faltando = CAMPOS - {"classificacao"} - set(entrada)
    if desconhecidos or faltando:
        raise ValueError(
            f"entrada de IBGE_AGREGADOS: campo desconhecido {sorted(desconhecidos)}, "
            f"faltando {sorted(faltando)}"
        )
    params = {"localidades": "N1[1]"}
    if "classificacao" in entrada:
        if len(entrada["classificacao"]) != 1:
            raise ValueError("a API v3 aceita uma classificação por chamada")
        [(classificacao, categorias)] = entrada["classificacao"].items()
        lista = "all" if categorias == "all" else ",".join(map(str, categorias))
        params["classificacao"] = f"{classificacao}[{lista}]"
    variaveis = "|".join(map(str, entrada["variaveis"]))
    return [
        HttpRequest(
            name=entrada["dataset"],
            endpoint=(
                f"/api/v3/agregados/{entrada['agregado']}"
                f"/periodos/-{entrada['periodos']}/variaveis/{variaveis}"
            ),
            params=params,
        )
    ]


CLASSIFICACOES = "resultados.classificacoes[*]"

AGREGADO = DatasetSpec(
    domain="ibge",
    dataset=lambda entrada: entrada["dataset"],
    extractor=api(base_url="https://servicodados.ibge.gov.br", requests=agregado),
    converter=ConverterConfig(
        nested=("resultados", "series"),
        explode_keys="serie",
        columns={
            "variavel_id": Field("item.id"),
            "variavel_nome": Field("item.variavel"),
            "localidade_id": Field("series.localidade.id"),
            "localidade_nome": Field("series.localidade.nome"),
            "classificacao_id": Field(f"{CLASSIFICACOES}.id", join="|", default="0"),
            "classificacao_nome": Field(f"{CLASSIFICACOES}.nome", join=" | ", default=""),
            "categoria_id": Field(
                f"{CLASSIFICACOES}.categoria{{keys}}", join="|", default="0"
            ),
            "categoria_nome": Field(
                f"{CLASSIFICACOES}.categoria{{values}}", join=" | ", default=""
            ),
            "unidade": Field("item.unidade"),
            "periodo": Field("key"),
            "valor": Field("value"),
        },
    ),
    load_mode=LoadMode.MERGE,
    keys=("variavel_id", "localidade_id", "classificacao_id", "categoria_id", "periodo"),
)


@dag(
    dag_id="ibge_ingest_dag",
    # Diário às 06:00: cada pesquisa tem o seu calendário; a janela cobre revisões.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Mateus", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ibge", "pib_construcao", "sinapi", "conjuntura", "ingestion"],
)
def ibge_ingest_dag() -> None:
    @task
    def extract_to_raw(**context: Any) -> dict[str, list[str]]:
        agregados = Variable.get("IBGE_AGREGADOS", deserialize_json=True)
        try:
            return steps.extract_to_raw(AGREGADO, context["dag_run"].run_after, agregados)
        except NothingNew as exc:
            raise AirflowSkipException(str(exc)) from exc

    @task
    def convert_to_staging(raw: dict[str, list[str]]) -> dict[str, list[str]]:
        agregados = Variable.get("IBGE_AGREGADOS", deserialize_json=True)
        return steps.convert_to_staging(AGREGADO, raw, agregados)

    convert_to_staging(extract_to_raw())


dag_instance = ibge_ingest_dag()
