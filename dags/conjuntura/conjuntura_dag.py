import os
from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import DBT_LOG_PATH_ENVVAR

dbt_log_path = "/tmp/dbt_logs"  # NOSONAR
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/mcid/profiles.yml",
    profile_name="mcid",
    target_name="prod",
)

# Ingestões que alimentam o conjuntura: gravam raw e staging no lake, e a bronze lê a
# staging pelo pg_duckdb. Toda DAG que escreve uma staging lida pelo conjuntura_dbt
# precisa estar aqui.
INGEST_DAG_IDS = [
    "abecip_financiamentos_ingest_dag",  # financiamentos SBPE por modalidade (pág. 2)
    "abecip_instituicoes_ingest_dag",  # abertura por instituição (pág. 3)
    "ibge_ingest_dag",  # PIB, SINAPI, PIM-PF, PMC
    "novo_caged_construcao_edificios",  # empregos: saldo + estoque (pág. 3)
    "novo_caged_total_construcao",  # empregos: total da construção (pág. 3)
    "novo_caged_servicos_especializados_construcao",  # empregos (pág. 3)
    # MRV lançamentos/vendas: fora da lista — as construtoras entram via
    # o dado manual `empresas` (balanços), que já engloba a MRV.
    "bacen_sgs_ingest_dag",
    "bacen_credito_pib_ingest_dag",  # Crédito Imobiliário / PIB (pág. 4)
    "abecip_poupanca_trimestral_ingest_dag",
    "incc_m_ingest_dag",
    "dotacao_execucao_outras_fontes_mcid_ingest_dag",  # OGU
    "infomoney_imob",
    "fipezap_trimestral_ingest_dag",
    "icst_ingest_dag",
]


@dag(
    dag_id="conjuntura_dag",
    schedule="0 8 * * 1",
    max_active_runs=1,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["conjuntura", "dbt", "orquestracao"],
    description=(
        "Orquestra o boletim de conjuntura (semanal): dispara as "
        "ingestões automatizadas (que geram os parquets de staging) e roda o dbt "
        "conjuntura_dbt (silver via pg_duckdb read_parquet + gold). "
        "DAGs de ingestão: " + ", ".join(INGEST_DAG_IDS)
    ),
)
def conjuntura_dag() -> None:
    """DAG guarda-chuva do boletim de conjuntura.

    Fluxo:
      1. Dispara as DAGs de ingestão em paralelo e espera todas, sem prender slot de
         worker: a espera é adiada para o triggerer (deferrable).
      2. Roda o dbt `conjuntura_dbt` (silver + gold do exercício atual).

    Os dados manuais (balanços das empresas e afins) chegam ao banco pelos scripts de
    scripts/database/ e a bronze lê a source `conjuntura_manual`: não há passo aqui.

    O dbt roda mesmo que alguma fonte falhe (trigger_rule=all_done): uma fonte
    instável não deve travar o boletim — os modelos daquela fonte ficam com o
    último parquet que deu certo.
    """
    fontes_prontas = EmptyOperator(task_id="fontes_prontas", trigger_rule="all_done")

    for ingest_dag_id in INGEST_DAG_IDS:
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{ingest_dag_id}",
            trigger_dag_id=ingest_dag_id,
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True,  # não segura um slot de worker por DAG enquanto espera
        )
        trigger >> fontes_prontas

    dbt_conjuntura = DbtTaskGroup(
        group_id="dbt_conjuntura",
        project_config=ProjectConfig(f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/mcid"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
        ),
        render_config=RenderConfig(select=["conjuntura_dbt"]),
    )

    fontes_prontas >> dbt_conjuntura


dag_instance = conjuntura_dag()
