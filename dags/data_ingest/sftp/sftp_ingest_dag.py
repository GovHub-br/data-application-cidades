import logging
import os
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import dag, get_current_context, task

from datalake import contrato
from datalake.chaves import load_id
from datalake.sftp import Control
from datalake.sftp import extract as extract_sftp
from datalake.tasks import Lake, previous_rows, record, summary
from datalake.tasks import transform as transform_key
from lake_utils import md5_arquivo

SFTP_CONN = "sftp_mcid"
LAKE_CONN = "lake_mcid"
PG_CONN = "postgres_default"


def _lake(dry_run: bool):
    return Lake(
        S3Hook(aws_conn_id=LAKE_CONN), os.environ["MINIO_BUCKET"], "dryrun/" if dry_run else ""
    )


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Gustavo", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False, "limit": 0},
    tags=["sftp", "mcid", "cidades", "data-lake", "ingest"],
)
def sftp_ingest_dag() -> None:
    """Ingestão do SFTP do MCid (ADR-0004): extract -> transform (um por arquivo) -> report.

    `extract` baixa os arquivos novos das pastas monitoradas, mascara a PII e só então sobe
    para `raw/sftp/<pasta>/load_id=<id>/` (o original nunca pousa). `transform` converte cada
    arquivo para a staging pelo leitor do contrato ODCS e segura em quarentena o que não
    bate com ele. `report` escreve o resumo no log da task e falha a execução se
    houve erro ou quarentena.

    Params: `dry_run` faz tudo embaixo de `dryrun/`; lê as tabelas de controle mas não grava;
    `limit` baixa no máximo N arquivos (0 = sem limite).
    """

    @task
    def extract() -> list:
        """Keys to convert as the return value (what `transform` maps over); the whole
        result goes to the `result` XCom, read by `report`."""
        ctx = get_current_context()
        dry = bool(ctx["params"]["dry_run"])
        sftp = SFTPHook(ssh_conn_id=SFTP_CONN)
        with sftp.get_managed_conn():  # one SSH session for the whole run
            result = extract_sftp(
                sftp,
                _lake(dry).upload_arquivo,
                Control(PostgresHook(postgres_conn_id=PG_CONN), ctx["run_id"], write=not dry),
                load_id(ctx["dag_run"].run_after),  # the same on a retry: it rewrites the same keys
                md5_arquivo,
                tmpdir=os.environ.get("LAKE_TMPDIR"),
                limit=int(ctx["params"]["limit"] or 0),
            )
        logging.info("[sftp_ingest_dag] extract: %s", {k: len(v) for k, v in result.items()})
        ctx["ti"].xcom_push(key="result", value=result)
        return result["keys"]

    @task(pool="lake_conversao")
    def transform(key: str) -> list:
        ctx = get_current_context()
        dry = bool(ctx["params"]["dry_run"])
        pg = PostgresHook(postgres_conn_id=PG_CONN)
        recs = transform_key(
            _lake(dry), key, ctx["run_id"], not dry, contrato.load(), previous_rows(pg)
        )
        if not dry:
            record(pg, recs)
        return [
            {k: r.get(k) for k in ("raw_key", "status", "structural", "error_message")}
            for r in recs
        ]

    @task(trigger_rule="all_done")
    def report(transformed: list) -> None:
        ctx = get_current_context()
        extracted = ctx["ti"].xcom_pull(task_ids="extract", key="result") or {
            "keys": [],
            "errors": ["a task extract falhou antes de terminar; veja o log dela"],
        }
        transformed = list(transformed or [])
        records = [rec for recs in transformed for rec in recs]
        body, failed, notable = summary(
            extracted, records, len(extracted["keys"]) - len(transformed)
        )
        # no e-mail service yet: the task log is the alert (error/warning stand out in the UI)
        log = logging.error if failed else logging.warning if notable else logging.info
        log("[sftp_ingest_dag] report\n%s", body)
        if failed:
            raise RuntimeError("ingestão do SFTP com erro ou quarentena; veja o resumo no log")

    report(transform.expand(key=extract()))


dag_instance = sftp_ingest_dag()
