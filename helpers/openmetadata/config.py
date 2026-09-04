"""Catálogo de recipes do OpenMetadata, caminhos e liga/desliga.

Base: a implementação que já existia em `origin/refactor/openmetadata`
(2026-08-18), que é Airflow 2 como o resto deste repositório. Refino: a forma
do `data-application-minc`, que portou esta mesma integração daqui e a evoluiu
— flags de liga/desliga por recipe e resolução de segredo no runtime da task,
em vez de Jinja no replacement.
"""

import os
from dataclasses import dataclass, field
from typing import Mapping

AIRFLOW_REPO_BASE = os.environ.get("AIRFLOW_REPO_BASE", "/opt/airflow")

OPENMETADATA_DIR = f"{AIRFLOW_REPO_BASE}/helpers/openmetadata"
OPENMETADATA_RECIPES_DIR = f"{OPENMETADATA_DIR}/recipes"
OPENMETADATA_GLOSSARY_PATH = f"{OPENMETADATA_DIR}/glossaries/mcid.yaml"
OPENMETADATA_SEMANTIC_RELATIONSHIPS_PATH = (
    f"{OPENMETADATA_DIR}/semantic_relationships/mcid.yaml"
)
#: O projeto dbt do MCID. A recipe de dbt consome os artefatos que saem dele.
DBT_MCID_DIR = f"{AIRFLOW_REPO_BASE}/dbt/mcid"

#: O pacote roda num VIRTUALENV ISOLADO, e isso não é preferência: o
#: `openmetadata-ingestion` exige `sqlalchemy>=2.0` e o `apache-airflow 2.8.1`
#: exige `<2.0`. Não existe versão do pacote que conviva com o Airflow 2 no
#: mesmo ambiente — testadas 1.10, 1.11, 1.12 e 1.13, todas incompatíveis.
#: O `data-application-minc` consegue assar na imagem porque está em Airflow
#: 3.2.2, que já usa SQLAlchemy 2.
#:
#: Isolado, o pacote resolve limpo em 255 pacotes. A versão acompanha a LINHA
#: do servidor (hoje 1.13.3): cliente e servidor de linhas diferentes divergem
#: no schema das entidades.
OPENMETADATA_REQUIREMENTS = [
    "openmetadata-ingestion[dbt,postgres,superset,airflow,pii-processor]==1.13.3.2",
    "asyncpg",
    "PyYAML>=6.0",
    "psycopg2-binary",
]


@dataclass(frozen=True)
class RecipeDefinition:
    task_id: str
    recipe_path: str
    command: str
    replacements: Mapping[str, str]
    #: marcador na recipe -> nome do segredo, resolvido no runtime da task
    segredos: Mapping[str, str] = field(default_factory=dict)
    dbt_project_dir: str = ""
    enabled: bool = True


def _flag(nome: str, *, default: bool) -> bool:
    """Liga ou desliga uma recipe por variável de AMBIENTE.

    Não é Variable do Airflow porque a decisão acontece no parse da DAG:
    buscar Variable a cada parse bate no banco de metadados a cada poucos
    segundos. O custo é recriar o container para mudar a flag, aceitável para
    algo que muda quando a infraestrutura muda.
    """
    bruto = os.environ.get(nome)
    if bruto is None:
        return default
    return bruto.strip().lower() in {"1", "true", "yes", "on", "sim"}


#: Metadados de tabela e de dbt são o núcleo e vêm ligados.
INGERIR_POSTGRES = _flag("OM_INGEST_POSTGRES", default=True)
INGERIR_DBT = _flag("OM_INGEST_DBT", default=True)
#: O Airflow é serviço compartilhado do laboratório; a ingestão dele traz as
#: DAGs de todos os projetos, não só as nossas.
INGERIR_AIRFLOW = _flag("OM_INGEST_AIRFLOW", default=True)
#: Superset exige credencial própria; desligado até ela existir.
INGERIR_SUPERSET = _flag("OM_INGEST_SUPERSET", default=False)
#: Profiler e classifier leem LINHAS do banco. Ficam desligados por padrão:
#: ligar é decisão de governança, não de configuração — ver a nota sobre
#: `storeSampleData` no README desta pasta.
INGERIR_PROFILER = _flag("OM_INGEST_PROFILER", default=False)
INGERIR_CLASSIFIER = _flag("OM_INGEST_CLASSIFIER", default=False)


#: `.get` com padrão de propósito: `os.environ[...]` levanta no PARSE da DAG, e
#: uma variável ausente derrubaria o arquivo inteiro em vez de a task falhar.
COMMON_REPLACEMENTS = {
    "DB_DW_HOST": os.environ.get("DB_DW_HOST_MCID", "postgres"),
    "DB_DW_PORT": os.environ.get("DB_DW_PORT_MCID", "5432"),
    "DB_DW_USER": os.environ.get("DB_DW_USER_MCID", "postgres_dw"),
    "DB_DW_PASSWORD": os.environ.get("DB_DW_PASSWORD_MCID", "postgres_dw"),
    "DB_DW_DBNAME": os.environ.get("DB_DW_DBNAME_MCID", "data_warehouse"),
    # nome do serviço no compose, não `localhost`: dentro do container
    # `localhost` é o próprio container, e só funciona por acidente quando
    # webserver e scheduler estão juntos.
    "AIRFLOW_HOST_PORT": os.environ.get("AIRFLOW_HOST_PORT", "http://airflow:8080"),
    "AIRFLOW_DB_HOST_PORT": os.environ.get("AIRFLOW_DB_HOST_PORT", "postgres:5432"),
    "AIRFLOW_DB_USERNAME": os.environ.get("POSTGRES_USER", "airflow"),
    "AIRFLOW_DB_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    "AIRFLOW_DB_DATABASE": os.environ.get("AIRFLOW_DB_DATABASE", "airflow"),
}

#: Segredos lidos do AMBIENTE dentro da task. Não são Variable do Airflow
#: porque a task roda num virtualenv sem o pacote `airflow` instalado
#: (`expect_airflow=False`): lá dentro não há `Variable` para consultar. O
#: ambiente, esse sim, é herdado.
#:
#: Os nomes são os mesmos que `scripts/governance/` já lê do `.env`, que o
#: compose monta via `env_file`. Uma fonte só evita o token divergir entre dois
#: lugares — problema que já corrigimos uma vez com o proprietário.
SEGREDOS_BASE = {"OM_HOST": "OPENMETADATA_URL"}
SEGREDOS_INGESTAO = {**SEGREDOS_BASE, "INGESTION_TOKEN": "OPENMETADATA_JWT_TOKEN"}
SEGREDOS_PROFILER = {**SEGREDOS_BASE, "PROFILER_TOKEN": "OPENMETADATA_JWT_TOKEN"}
SEGREDOS_CLASSIFIER = {**SEGREDOS_BASE, "CLASSIFICATION_TOKEN": "OPENMETADATA_JWT_TOKEN"}
SEGREDOS_SUPERSET = {
    **SEGREDOS_INGESTAO,
    "SUPERSET_HOST_PORT": "SUPERSET_HOST_PORT",
    "SUPERSET_USERNAME": "SUPERSET_USERNAME",
    "SUPERSET_PASSWORD": "SUPERSET_PASSWORD",
}


AIRFLOW_METADATA_RECIPE = RecipeDefinition(
    task_id="airflow_metadata",
    enabled=INGERIR_AIRFLOW,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/airflow_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    segredos=SEGREDOS_INGESTAO,
)

POSTGRES_METADATA_RECIPE = RecipeDefinition(
    task_id="postgres_metadata",
    enabled=INGERIR_POSTGRES,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    segredos=SEGREDOS_INGESTAO,
)

DBT_METADATA_RECIPE = RecipeDefinition(
    task_id="dbt_metadata",
    enabled=INGERIR_DBT,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/dbt_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    segredos=SEGREDOS_INGESTAO,
    dbt_project_dir=DBT_MCID_DIR,
)

POSTGRES_PROFILER_RECIPE = RecipeDefinition(
    task_id="postgres_profiler",
    enabled=INGERIR_PROFILER,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_profiler.yaml",
    command="profile",
    replacements=COMMON_REPLACEMENTS,
    segredos=SEGREDOS_PROFILER,
)

POSTGRES_CLASSIFIER_RECIPE = RecipeDefinition(
    task_id="postgres_classifier",
    enabled=INGERIR_CLASSIFIER,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_classifier.yaml",
    command="classify",
    replacements=COMMON_REPLACEMENTS,
    segredos=SEGREDOS_CLASSIFIER,
)

SUPERSET_METADATA_RECIPE = RecipeDefinition(
    task_id="superset_metadata",
    enabled=INGERIR_SUPERSET,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/superset_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    segredos=SEGREDOS_SUPERSET,
)

_RECIPES_DEFINIDAS = (
    AIRFLOW_METADATA_RECIPE,
    POSTGRES_METADATA_RECIPE,
    DBT_METADATA_RECIPE,
    POSTGRES_PROFILER_RECIPE,
    POSTGRES_CLASSIFIER_RECIPE,
    SUPERSET_METADATA_RECIPE,
)

ALL_RECIPES = tuple(recipe for recipe in _RECIPES_DEFINIDAS if recipe.enabled)

#: A ORDEM É OBRIGATÓRIA. `postgres_metadata` cria as tabelas no catálogo;
#: `dbt_metadata` só ANEXA descrição, tier, domínio e linhagem a tabela que já
#: existe — ele não cria nada. Profiler e classifier só têm o que medir depois
#: das duas. Inverter faz o dbt anexar a nada e não dá erro nenhum.
RECIPE_PIPELINE = (
    "airflow_metadata",
    "postgres_metadata",
    "dbt_metadata",
    "postgres_profiler",
    "postgres_classifier",
    "superset_metadata",
)
