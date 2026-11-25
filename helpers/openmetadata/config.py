"""Catálogo de recipes do OpenMetadata, caminhos e liga/desliga.

Base: a implementação que já existia em `origin/refactor/openmetadata`
(2026-08-18), escrita para o Airflow 2. Refino: a forma
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
OPENMETADATA_SEMANTIC_RELATIONSHIPS_PATH = f"{OPENMETADATA_DIR}/semantic_relationships/mcid.yaml"
#: O projeto dbt do MCID. A recipe de dbt consome os artefatos que saem dele.
DBT_MCID_DIR = f"{AIRFLOW_REPO_BASE}/dbt/mcid"

#: O `openmetadata-ingestion` está na IMAGEM do Airflow, pelo lock
#: (`infra/docker/airflow/compile-lock.sh`), e não num virtualenv. O venv
#: existia porque instalar o pacote solto por cima da base rebaixava pacotes
#: dela; o lock resolve tudo de uma vez, como no `data-application-minc`. No
#: venv, com o Airflow 3.2.2 que o extra `airflow` instalava, a recipe
#: `airflow_metadata` falhava no homolog ao inicializar o ORM; na imagem, o
#: conector roda no mesmo Airflow da task. Ver o README desta pasta.
#: A versão do pacote acompanha a LINHA do servidor (ver o grupo runtime do `pyproject.toml`).


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

#: As relações semânticas vêm ligadas desde que o catálogo
#: `semantic_relationships/mcid.yaml` foi regenerado sobre o medalhão
#: (2026-11-02): as 201 tabelas de prata e ouro com modelo no repo, medidas por
#: contagem exata no PostgreSQL. Ficaram desligadas enquanto o arquivo era o
#: snapshot de 2026-07-23, que nem passava no `load_semantic_catalog`.
#:
#: O catálogo é foto do dado do dia da medição. Tabela nova ou renomeada fica
#: sem relação até a próxima rodada; tabela que saiu do OpenMetadata derruba a
#: task (`sync_semantic_relationships` exige todas as do escopo), e aí basta
#: desligar com `OM_SYNC_RELACOES_SEMANTICAS=false` até regenerar — a
#: reaplicação de governança roda mesmo assim (`all_done`).
SINCRONIZAR_RELACOES_SEMANTICAS = _flag("OM_SYNC_RELACOES_SEMANTICAS", default=True)


#: Marcador da recipe -> variável de ambiente que o preenche. Os valores não vêm do
#: `.env`: a DAG lê as Connections (`postgres_default`, `openmetadata_default`,
#: `superset`) e a configuração do próprio Airflow dentro da task e exporta essas
#: variáveis no processo dela, que as recipes, o `dbt docs generate` e os scripts de
#: `scripts/governance/` herdam. Nada é lido no parse da DAG.
COMMON_REPLACEMENTS = {
    "DB_DW_HOST": "DB_DW_HOST_MCID",
    "DB_DW_PORT": "DB_DW_PORT_MCID",
    "DB_DW_USER": "DB_DW_USER_MCID",
    "DB_DW_PASSWORD": "DB_DW_PASSWORD_MCID",
    "DB_DW_DBNAME": "DB_DW_DBNAME_MCID",
    "AIRFLOW_HOST_PORT": "AIRFLOW_HOST_PORT",
    "AIRFLOW_DB_HOST_PORT": "AIRFLOW_DB_HOST_PORT",
    "AIRFLOW_DB_USERNAME": "AIRFLOW_DB_USERNAME",
    "AIRFLOW_DB_PASSWORD": "AIRFLOW_DB_PASSWORD",
    "AIRFLOW_DB_DATABASE": "AIRFLOW_DB_DATABASE",
}

#: Os segredos seguem a mesma regra: marcador -> variável exportada pela task.
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
#:
#: `airflow_metadata` vai por ÚLTIMO porque nenhuma outra depende dele, e a
#: corrente é sequencial: na frente, uma falha dele marcava `postgres_metadata`
#: e `dbt_metadata` como `upstream_failed`, e o catálogo do banco parava de
#: atualizar por causa de um conector secundário.
RECIPE_PIPELINE = (
    "postgres_metadata",
    "dbt_metadata",
    "postgres_profiler",
    "postgres_classifier",
    "superset_metadata",
    "airflow_metadata",
)
