import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR
from schedule_loader import get_dynamic_schedule

dbt_log_path = "/tmp/dbt_logs"  # NOSONAR
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/mcid/profiles.yml",
    profile_name="mcid",
    target_name="prod",
)

# DAGs de ingestão que alimentam as fontes usadas pelos modelos
# conjuntura_dbt (schemas raw: abecip, bacen, fgv, fipe, ibge, infomoney).
INGEST_DAG_IDS = [
    "abecip_poupanca_trimestral_ingest_dag",
    "bacen_sgs_ingest_dag",
    "icst_ingest_dag",
    "incc_m_ingest_dag",
    "fipezap_trimestral_ingest_dag",
    "ibge_ingest_dag",
    "infomoney_imob",
]


@dag(
    dag_id="conjuntura_boletim_dag",
    schedule_interval=get_dynamic_schedule("conjuntura_boletim_dag", default="0 10 * * 5"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["conjuntura", "boletim", "dbt", "orquestracao"],
    description=(
        "Orquestra todas as ingestões usadas pelo boletim de conjuntura e, "
        "em seguida, roda os modelos dbt de conjuntura_dbt (bronze/silver/gold)."
    ),
)
def conjuntura_boletim_dag() -> None:
    """DAG guarda-chuva do boletim de conjuntura.

    Dispara em paralelo as DAGs de ingestão das fontes do boletim (ABECIP,
    BACEN, FGV, FipeZap, IBGE e Infomoney), aguarda a conclusão de todas e só
    então roda o dbt (`conjuntura_dbt`), garantindo que as camadas
    bronze/silver/gold sejam construídas com os dados mais recentes.

    O dbt roda mesmo que alguma ingestão falhe (trigger_rule=all_done): uma
    fonte externa instável não deve travar o boletim inteiro — os modelos
    daquela fonte simplesmente ficam com os dados da última ingestão que deu
    certo.
    """
    ingestoes_concluidas = EmptyOperator(
        task_id="ingestoes_concluidas", trigger_rule="all_done"
    )

    for ingest_dag_id in INGEST_DAG_IDS:
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{ingest_dag_id}",
            trigger_dag_id=ingest_dag_id,
            wait_for_completion=True,
            poke_interval=60,
        )
        trigger >> ingestoes_concluidas

    dbt_conjuntura = DbtTaskGroup(
        group_id="dbt_conjuntura",
        project_config=ProjectConfig(f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/mcid"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
        ),
        render_config=RenderConfig(select=["conjuntura_dbt"]),
    )

    ingestoes_concluidas >> dbt_conjuntura


dag_instance = conjuntura_boletim_dag()
