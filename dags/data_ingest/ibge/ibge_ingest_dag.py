"""IBGE, API v3 de agregados: PIB, SINAPI, PAIC, PNAD-C, PIM-PF e PMC.

Fonte: `servicodados.ibge.gov.br/api/v3/agregados/<agregado>/periodos/<janela>/
variaveis/<v1|v2...>`, Brasil (`N1[1]`), com uma classificação quando a série
pede. Uma tabela por agregado; a lista mora aqui (antes vinha da Variable
`IBGE_CONFIGURACOES`, lida no parse, em que uma vírgula a mais derrubava a DAG
inteira). Mudar um agregado é PR.

A API separa variáveis por `|` e categorias por `,`, e recusa (HTTP 500) duas
classificações na mesma chamada: uma classificação por tabela.

LoadMode: merge pelas colunas do achatamento (variável, localidade,
classificação, categoria, período). Cada chamada traz só a janela pedida (`-20`,
`-30`…), então o histórico se acumula pelas ingestões, e revisões do IBGE
dentro da janela substituem o valor anterior.

Conversão: o `JsonConverter` com o formato da resposta declarado aqui (`IBGE_V3`):
cada variável tem `resultados` (uma combinação de categorias) e, neles, uma `serie`
por localidade com o PERÍODO COMO CHAVE. Uma linha por (variável, localidade,
classificação, categoria, período), em texto; sem classificação, ids `"0"` e nomes
vazios; com várias, ids juntados por `|` e nomes por ` | ` (o achatamento que o
macro `ibge_v3_tipado` e as chaves do merge supõem). A tipagem que a DAG antiga
fazia em pandas é da prata (macro `ibge_v3_tipado`).
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import TaskGroup, dag, task

from ingestion.converters import ConverterConfig, Field
from ingestion.dataset import DatasetSpec
from ingestion.extractors import ExtractorConfig, HttpRequest
from ingestion.loaders import LoadMode
from ingestion.pipeline import steps

# (tabela, agregado, variáveis, janela de períodos, classificação[categorias])
AGREGADOS: tuple[tuple[str, int, str, str, str | None], ...] = (
    ("pib_construcao", 5932, "6564|6563|6562|6561", "-20", "11255[90694]"),
    ("sinapi", 2296, "48|1196|1197|1198", "-30", None),
    ("pib_consolidado_trimestral_bruto", 2072, "933", "-20", None),
    ("pib_corrente_milhoes_brl", 1846, "585", "-20", "11255[all]"),
    ("paic_resultados", 585, "632|1908|1924", "-10", None),
    ("paic_pessoal_salarial", 586, "631|1816|673|1780", "-10", None),
    ("paic_obras", 591, "1930|1931|1932", "-10", None),
    ("pnad_trabalho_construcao", 6323, "4090", "-12", "888[47946,47949]"),
    ("pnad_rendimento_construcao", 6391, "5932", "-12", "888[47946,47949]"),
    ("pnadc_populacao_decis_renda", 7521, "606", "-5", "1019[all]"),
    ("pnadc_rendimento_domiciliar_real", 7531, "10824", "-5", "1019[all]"),
    ("pnadc_habitacao_condicao", 6821, "162|10114", "-5", "63[4343,1055,2519,1058]"),
    ("ibge_pim_pf_brasil", 8886, "12606|11602|11603|11604", "-30", None),
    (
        "ibge_pmc_construcao",
        8757,
        "7169|7170|11708|11709|11710|11711",
        "-30",
        "11046[56732]",
    ),
)
KEYS = ("variavel_id", "localidade_id", "classificacao_id", "categoria_id", "periodo")
CLASSIFICACOES = "resultados.classificacoes[*]"
IBGE_V3 = ConverterConfig(
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
)


def _spec(
    tabela: str, agregado: int, variaveis: str, periodos: str, classificacao: str | None
) -> DatasetSpec:
    params = {"localidades": "N1[1]"}
    if classificacao:
        params["classificacao"] = classificacao
    return DatasetSpec(
        domain="ibge",
        dataset=tabela,
        extractor=ExtractorConfig(
            source="api",
            base_url="https://servicodados.ibge.gov.br",
            requests=(
                HttpRequest(
                    name=tabela,
                    endpoint=(
                        f"/api/v3/agregados/{agregado}/periodos/{periodos}"
                        f"/variaveis/{variaveis}"
                    ),
                    params=params,
                ),
            ),
        ),
        converter=IBGE_V3,
        load_mode=LoadMode.MERGE,
        keys=KEYS,
    )


DATASETS = tuple(_spec(*agregado) for agregado in AGREGADOS)


def _pipeline(spec: DatasetSpec) -> None:
    @task(task_id="extract_to_raw")
    def extract_to_raw(**context: Any) -> str:
        return steps.extract_to_raw(spec, context["dag_run"].run_after)

    @task(task_id="convert_to_staging")
    def convert_to_staging(raw_prefix: str) -> str:
        return steps.convert_to_staging(spec, raw_prefix)

    convert_to_staging(extract_to_raw())


@dag(
    dag_id="ibge_ingest_dag",
    # Diário às 06:00: cada pesquisa tem o seu calendário; a janela cobre revisões.
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ibge", "pib_construcao", "sinapi", "conjuntura", "ingestion"],
)
def ibge_ingest_dag() -> None:
    # Uma tabela que falha não bloqueia as outras: cada grupo é independente.
    for spec in DATASETS:
        with TaskGroup(group_id=spec.dataset):
            _pipeline(spec)


dag_instance = ibge_ingest_dag()
