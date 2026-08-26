import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import DBT_LOG_PATH_ENVVAR
from schedule_loader import get_dynamic_schedule
from ingestor_lake import IngestorBalancoEmpresas

dbt_log_path = "/tmp/dbt_logs"  # NOSONAR
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/mcid/profiles.yml",
    profile_name="mcid",
    target_name="prod",
)

# Ingestões automatizadas que alimentam o conjuntura contínuo. São as mesmas
# fontes do boletim trimestral: além de carregar o Postgres, agora também geram
# o parquet tipado na staging (Etapa 02) consumido pela silver via pg_duckdb.
INGEST_DAG_IDS = [
    "ibge_ingest_dag",  # PIB, SINAPI, PIM-PF, PMC
    "ibge_pnad_construcao_sidra_ingest_dag",  # PNAD-C ocupados+rendimento (pág. 3)
    "novo_caged_construcao_edificios",  # empregos: saldo + estoque (pág. 3)
    # MRV lançamentos/vendas: fora do contínuo — as construtoras entram via
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

# Ingestores manuais (Template Method): convertem arquivos já colocados no RAW
# (CSV/XLSX/TXT) em parquet tipado na staging. Adicionar novas classes aqui
# conforme os dados manuais forem chegando (ver README do conjuntura_continuo_dbt).
INGESTORES_MANUAIS = [
    IngestorBalancoEmpresas,
]


@dag(
    dag_id="conjuntura_continuo_dag",
    schedule_interval=get_dynamic_schedule(
        "conjuntura_continuo_dag", default="0 8 * * 1"
    ),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["conjuntura", "continuo", "dbt", "orquestracao"],
    description=(
        "Orquestra o boletim de conjuntura CONTÍNUO (semanal): dispara as "
        "ingestões automatizadas (que geram os parquets de staging), converte os "
        "dados manuais para parquet tipado (Template Method) e roda o dbt "
        "conjuntura_continuo_dbt (silver via pg_duckdb read_parquet + gold). "
        "DAGs de ingestão: " + ", ".join(INGEST_DAG_IDS)
    ),
)
def conjuntura_continuo_dag() -> None:
    """DAG guarda-chuva do boletim de conjuntura contínuo.

    Fluxo:
      1. Dispara as DAGs de ingestão automatizadas (Etapas 01/02 das fontes com
         API) em paralelo e aguarda a conclusão de todas.
      2. Gera os parquets tipados dos dados manuais já presentes no RAW.
      3. Roda o dbt `conjuntura_continuo_dbt` (silver + gold do exercício atual).

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
        )
        trigger >> fontes_prontas

    @task(trigger_rule="all_done")
    def gera_parquets_manuais() -> None:
        """Etapa 02 dos dados manuais: roda cada Template Method configurado.

        Uma falha isolada (ex.: arquivo ainda não colocado no RAW) apenas loga
        e segue — o dado fica com o último parquet válido.
        """
        for ingestor_cls in INGESTORES_MANUAIS:
            ingestor = ingestor_cls()
            try:
                ingestor.gerar_staging_parquet()
                logging.info(f"Parquet manual gerado: {ingestor.fonte}.{ingestor.dado}")
            except Exception as exc:  # noqa: BLE001 - fonte externa instável
                logging.warning(
                    f"Ingestor manual falhou ({ingestor.fonte}.{ingestor.dado}): "
                    f"{exc}"
                )

    manuais_prontos = gera_parquets_manuais()

    dbt_conjuntura_continuo = DbtTaskGroup(
        group_id="dbt_conjuntura_continuo",
        project_config=ProjectConfig(f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/mcid"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
        ),
        render_config=RenderConfig(select=["conjuntura_continuo_dbt"]),
    )

    [fontes_prontas, manuais_prontos] >> dbt_conjuntura_continuo


dag_instance = conjuntura_continuo_dag()
