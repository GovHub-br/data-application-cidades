"""DAG do dbt do MCid.

Um DAG, DOIS task groups — porque o projeto tem dois dialetos de SQL:

- far_dbt / fds_dbt / rural_dbt / conjuntura_dbt / metadata e as seeds são SQL de
  Postgres. Usam as UDFs `normalize_apf` / `parse_date_br` / `f_corrigir_mojibake`,
  criadas pelo `create_udfs()` com CREATE FUNCTION no on-run-start.
- mcmv_historico_dbt é SQL NATIVO do DuckDB: `try_cast(x as tipo)`,
  `union_by_name = true`, `describe select * from read_parquet(...)`. O parser do
  Postgres rejeita tudo isso — não é um bug do modelo, é outro motor.

Rodar os dois no mesmo target é impossível sem reescrever um dos lados. Em vez
disso, cada task group carrega seu próprio ProfileConfig:

    prod          -> Postgres direto
    prod_duckdb   -> motor DuckDB no processo do dbt, lendo o MinIO e escrevendo
                     no Postgres atachado como catálogo `cidades`

A ordem importa: o histórico faz `ref('prata_fds_dim_empreendimento')`, que é
materializada pelo grupo do Postgres. Como ela fica FORA da seleção do segundo
grupo, o dbt só resolve a referência — não tenta reconstruí-la com as UDFs que
não existem no DuckDB. As seeds também são carregadas no primeiro grupo e lidas
pelo segundo através do catálogo atachado.

Pré-requisitos no ambiente do Airflow (além do dbt-postgres):
- `dbt-duckdb` instalado, na linha 1.7 (o dbt-postgres 1.7.13 fixa dbt-core 1.7.13);
- variáveis do MinIO — MINIO_ENDPOINT (host:porta, SEM http://), MINIO_ACCESS_KEY,
  MINIO_SECRET_KEY — visíveis para o worker;
- um pool chamado `duckdb_historico` com UM slot:

      airflow pools set duckdb_historico 1 "Serializa o eixo historico (lock DuckDB)"

  Sem o pool as tasks falham com "Pool not found"; com mais de um slot elas voltam
  a colidir no lock do arquivo .duckdb (ver POOL_DUCKDB abaixo).
"""

import os
from datetime import datetime

from airflow import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR

dbt_log_path = "/tmp/dbt_logs"  # NOSONAR
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

REPO_BASE = os.environ["AIRFLOW_REPO_BASE"]
PROJECT_DIR = f"{REPO_BASE}/dbt/mcid"
PROFILES_YML = f"{PROJECT_DIR}/profiles.yml"

# O eixo histórico: os modelos e os testes singulares que dependem deles.
# A mesma lista serve para EXCLUIR do grupo Postgres e SELECIONAR no grupo DuckDB,
# então os dois grupos nunca se sobrepõem nem deixam nó órfão.
SELETOR_HISTORICO = ["path:models/mcmv_historico_dbt", "path:tests/mcmv_historico"]

# O DuckDB nao aceita duas conexoes de ESCRITA no mesmo arquivo. O Cosmos roda uma
# invocacao de dbt por task, cada uma abrindo sua propria conexao em
# /tmp/mcid_local.duckdb -- entao duas tasks irmas em paralelo derrubam a segunda
# com "Could not set lock on file". O --threads do dbt nao resolve: ele limita a
# concorrencia DENTRO de uma invocacao, e aqui quem paraleliza e o Airflow.
# Um pool de 1 slot serializa o grupo inteiro. Custa minutos (sao ~25 modelos) e as
# tres bronzes pesadas da serie executiva ja precisavam rodar sozinhas por memoria.
POOL_DUCKDB = "duckdb_historico"

# partial_parse LIGADO (padrão). Cheguei a desligá-lo para evitar que um manifest
# em cache de um target fosse aplicado ao outro — mas isso deixou cada `dbt ls` da
# renderização lento o bastante para estourar o dagbag_import_timeout, e o risco
# deixou de existir quando o macros/get_custom_schema.sql passou a rotear schema
# igual nos três targets de produção: o manifest resolve o mesmo endereço para
# `prod` e `prod_duckdb`.
project_config = ProjectConfig(PROJECT_DIR)

execution_config = ExecutionConfig(
    dbt_executable_path=f"{REPO_BASE}/.local/bin/dbt",
)

profile_postgres = ProfileConfig(
    profiles_yml_filepath=PROFILES_YML,
    profile_name="mcid",
    target_name="prod",
)

profile_duckdb = ProfileConfig(
    profiles_yml_filepath=PROFILES_YML,
    profile_name="mcid",
    target_name="prod_duckdb",
)

with DAG(
    dag_id="mcid_cosmos_dag",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    doc_md=__doc__,
) as my_cosmos_dag:

    dominios_postgres = DbtTaskGroup(
        group_id="dominios_postgres",
        project_config=project_config,
        profile_config=profile_postgres,
        execution_config=execution_config,
        render_config=RenderConfig(exclude=SELETOR_HISTORICO),
    )

    eixo_historico = DbtTaskGroup(
        group_id="eixo_historico",
        project_config=project_config,
        profile_config=profile_duckdb,
        execution_config=execution_config,
        render_config=RenderConfig(select=SELETOR_HISTORICO),
        operator_args={"pool": POOL_DUCKDB},
    )

    dominios_postgres >> eixo_historico
