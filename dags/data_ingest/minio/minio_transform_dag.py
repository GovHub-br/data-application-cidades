import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from schedule_loader import get_dynamic_schedule


@dag(
    schedule_interval=get_dynamic_schedule("minio_transform_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Gustavo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "dry_run": False,
        "limit": 0,
        "pattern": "",
        "only_ext": "",
        "max_size_mb": 0,
    },
    tags=["minio", "mcid", "cidades", "data-lake", "transform"],
)
def minio_transform_dag() -> None:
    """Pipeline de transformação do data lake: raw/ -> mascaramento PII -> staging/.

    Roda sobre o que estiver em raw/ no MinIO, INDEPENDENTE de quem carregou (SFTP hoje,
    outras fontes no futuro). Desacoplada da ingestão: schedule próprio, sem referência
    às DAGs de fonte.

    Duas etapas encadeadas, cada uma idempotente/incremental (tabelas de controle no
    schema `lake` do Postgres), então execuções agendadas só processam o que é novo.
    `max_active_runs=1` porque o mascaramento sobrescreve raw/ in-place — duas execuções
    concorrentes seriam corrida destrutiva. O I/O usa os clientes de plugins/.

    A carga para o warehouse NÃO faz parte desta DAG: só um subconjunto do lake vira
    tabela no Postgres, e essa seleção é feita pelos modelos dbt, não por varredura da
    staging inteira.

    Params (para disparo manual; o schedule usa os defaults e processa tudo):

    - `dry_run`  marque para NÃO sobrescrever nada: a prévia do mascaramento vai para
                 `masked_dryrun/` e a dos parquets para `staging_dryrun/`, e as tabelas de
                 controle não registram conversão. É o modo de conferir o pipeline.
    - `limit`    processa no máximo N objetos por task (0 = sem limite).
    - `pattern`  só objetos cuja key contenha esta substring.
    - `only_ext` só estas extensões, separadas por vírgula (ex.: "csv,txt").
    - `max_size_mb` pula objetos maiores que N MB (0 = sem limite). Serve para adiar os
                 poucos arquivos gigantes do lake (os dois do CadÚnico somam 54 GB) e
                 carregar primeiro a cauda leve; como as duas etapas são idempotentes por
                 hash, a execução seguinte sem o limite só processa o que ficou de fora.

    O default é execução real porque é o que o schedule precisa. Vale lembrar que o
    mascaramento sobrescreve o objeto no lugar e é irreversível na prática: depois da tag
    `masked=true`, o valor original só existe na origem.
    """

    @task
    def mascarar_pii() -> None:
        """Mascara PII (CPF/NIS/nome/endereço) nos objetos novos de raw/, in-place."""
        from mascarar_minio import run as run_mask

        p = get_current_context()["params"]
        contagem = run_mask(
            apply=not bool(p.get("dry_run", False)),
            limit=int(p.get("limit") or 0),
            pattern=str(p.get("pattern") or ""),
            only_ext=str(p.get("only_ext") or ""),
            max_size_mb=int(p.get("max_size_mb") or 0),
        )
        logging.info("[minio_transform_dag] mascaramento concluído: %s", contagem)

    @task
    def raw_para_staging() -> None:
        """Converte os objetos de raw/ para Parquet full-text em staging/.

        Pula raw/dados_historicos/ inteira (ver PASTAS_IGNORADAS em raw_para_staging.py):
        já tem tratamento próprio de um membro da equipe para ciência de dados, e gerar
        parquet full-text por cima seria trabalho duplicado. Continua sendo mascarada
        normalmente pela task anterior.
        """
        from raw_para_staging import run as run_staging

        p = get_current_context()["params"]
        contagem = run_staging(
            apply=not bool(p.get("dry_run", False)),
            limit=int(p.get("limit") or 0),
            pattern=str(p.get("pattern") or ""),
            only_ext=str(p.get("only_ext") or ""),
            max_size_mb=int(p.get("max_size_mb") or 0),
        )
        logging.info("[minio_transform_dag] raw->staging concluído: %s", contagem)

    # A ordem é obrigatória, não uma preferência: a staging lê raw/ como está, então
    # converter antes de mascarar publicaria PII em parquet.
    mascarar_pii() >> raw_para_staging()


dag_instance = minio_transform_dag()
