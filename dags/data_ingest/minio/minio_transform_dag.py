import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    # A DAG diária do SFTP a aciona e aguarda antes de materializar as Golds.
    schedule=None,
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
        "max_size_mb": 512,
    },
    tags=["minio", "mcid", "cidades", "data-lake", "transform"],
)
def minio_transform_dag() -> None:
    """Pipeline seguro: raw/ -> masked/ -> staging/.

    O raw é preservado como evidência. Somente a cópia em ``masked/`` é mascarada e
    convertida para Parquet. É chamada pela DAG diária de SFTP, sem cron concorrente.

    Duas etapas encadeadas, cada uma idempotente/incremental (tabelas de controle no
    schema `lake` do Postgres), então execuções agendadas só processam o que é novo.
    `max_active_runs=1` evita corrida entre cópia, máscara e staging.

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

    A fila cotidiana limita objetos a 512 MB; os cortes grandes do CadÚnico seguem pela
    DAG dedicada, no mesmo encadeamento diário.
    """

    @task
    def copiar_para_mascarado() -> None:
        from copiar_raw_para_mascarado import run as run_copy

        p = get_current_context()["params"]
        contagem = run_copy(
            limit=int(p.get("limit") or 0),
            pattern=str(p.get("pattern") or ""),
            only_ext=str(p.get("only_ext") or ""),
            max_size_mb=int(p.get("max_size_mb") or 0),
        )
        logging.info("[minio_transform_dag] raw->masked: %s", contagem)

    @task
    def mascarar_pii() -> None:
        """Mascara PII somente na área protegida ``masked/``."""
        from mascarar_minio import run as run_mask

        p = get_current_context()["params"]
        contagem = run_mask(
            apply=not bool(p.get("dry_run", False)),
            limit=int(p.get("limit") or 0),
            pattern=str(p.get("pattern") or ""),
            only_ext=str(p.get("only_ext") or ""),
            max_size_mb=int(p.get("max_size_mb") or 0),
            prefix="masked/",
        )
        logging.info("[minio_transform_dag] mascaramento concluído: %s", contagem)

    @task
    def raw_para_staging() -> None:
        """Converte objetos já mascarados para Parquet em staging/.

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
            prefix="masked/",
        )
        logging.info("[minio_transform_dag] raw->staging concluído: %s", contagem)

    # A ordem é obrigatória, não uma preferência: a staging lê raw/ como está, então
    # converter antes de mascarar publicaria PII em parquet.
    copia = copiar_para_mascarado()
    mascaramento = mascarar_pii()
    staging = raw_para_staging()
    copia >> mascaramento >> staging


dag_instance = minio_transform_dag()
