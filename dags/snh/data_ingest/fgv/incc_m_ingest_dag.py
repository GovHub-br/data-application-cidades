import json
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from plugins.datalake.config import fail_fast_on_missing_config
from plugins.datalake import contract, readers
from plugins.datalake.api import staging_key, staging_record, write_text_parquet
from plugins.datalake.keys import RAW_PREFIX, load_id, load_id_of, raw_key
from plugins.datalake.tasks import previous_rows, record, summary
from scripts.lake_utils import md5_file

# Link estável da série histórica no Sinduscon-PR; redireciona para o XLSX vigente.
ENDPOINT = "economia/indices-economicos/incc-m-fgv/9547-serie-historica-incc-m-fgv/"
FOLDER = "fgv/incc_m"  # snh/raw/fgv/incc_m/load_id=<id>/incc_m.xlsx
FILE_NAME = "incc_m.xlsx"


def _run_dir() -> Path:
    """LAKE_TMPDIR/<dag_id>/<run_id>/: os arquivos da execução, entre uma task e outra.

    shortcut: só funciona porque todo ambiente usa LocalExecutor com o LAKE_TMPDIR
    compartilhado; com outro executor, o extract passa a gravar no bucket.
    """
    ctx = get_current_context()
    run = re.sub(r"[^A-Za-z0-9_.-]", "_", ctx["run_id"])
    path = Path(os.environ.get("LAKE_TMPDIR") or "/tmp") / ctx["dag"].dag_id / run
    path.mkdir(parents=True, exist_ok=True)
    return path


def _lake() -> tuple[S3Hook, str, str]:
    """(hook, bucket, prefixo): o `dry_run` grava tudo embaixo de dryrun/."""
    dry = bool(get_current_context()["params"]["dry_run"])
    return S3Hook(aws_conn_id="lake_mcid"), Variable.get("lake_bucket"), "dryrun/" if dry else ""


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Gustavo", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False},
    tags=["fgv", "incc_m", "construcao", "data-lake", "ingest"],
)
def incc_m_ingest_dag() -> None:  # noqa: C901 - as tasks da DAG ficam dentro dela
    """INCC-M (FGV, publicado pelo Sinduscon-PR) para o lake (ADR-0004).

    extract → upload_to_s3 → transform → verify_structural_data_drift → upload_staging
    → report, e cleanup no fim. Cada carga traz a série inteira (carga completa).

    Param `dry_run`: grava tudo embaixo de `dryrun/` e não escreve nas tabelas de controle.
    """

    @task(retries=2, retry_exponential_backoff=True, execution_timeout=timedelta(minutes=30))
    @fail_fast_on_missing_config
    def extract() -> str:
        """O XLSX da série histórica pelo HttpHook (Connection `sinduscon`, com o
        User-Agent de navegador no extra, sem o qual o site recusa)."""
        response = HttpHook(method="GET", http_conn_id="sinduscon").run(
            ENDPOINT, extra_options={"timeout": 120}
        )  # != 2xx falha
        if not response.content:
            raise AirflowFailException("o Sinduscon-PR devolveu o XLSX do INCC-M vazio")
        path = _run_dir() / FILE_NAME
        path.write_bytes(response.content)
        return str(path)

    @task(retries=2, retry_exponential_backoff=True)
    @fail_fast_on_missing_config
    def upload_to_s3(path: str) -> str:
        """O XLSX em raw/fgv/incc_m/load_id=<id>/; se igual à última carga, nada pousa.

        O md5 vai no metadado do objeto, e a próxima execução compara com ele (ADR-0005).
        """
        hook, bucket, prefix = _lake()
        load = load_id(get_current_context()["dag_run"].run_after)  # o mesmo num retry
        md5 = md5_file(path)
        client = hook.get_conn()
        previous_loads = sorted(
            {load_id_of(k) for k in hook.list_keys(bucket, f"{prefix}{RAW_PREFIX}{FOLDER}/") or []}
            - {load, None}
        )
        if previous_loads:
            previous_key = f"{prefix}{raw_key(FOLDER, FILE_NAME, previous_loads[-1])}"
            if client.head_object(Bucket=bucket, Key=previous_key)["Metadata"].get("md5") == md5:
                raise AirflowSkipException("o INCC-M não mudou desde a última carga")
        key = raw_key(FOLDER, FILE_NAME, load)
        client.upload_file(path, bucket, prefix + key, ExtraArgs={"Metadata": {"md5": md5}})
        return key

    @task
    def transform(key: str) -> dict:
        """A raw em Parquet local (texto + linhagem), pelo leitor do contrato `fgv_indices` (contracts/snh/fgv/indices.odcs.yaml)."""
        hook, bucket, prefix = _lake()
        run_id = get_current_context()["run_id"]
        dataset = contract.match(key, contract.load())

        if dataset is None:  # fica na raw e vai para o resumo (ADR-0006)
            return staging_record(run_id, key, status="uncataloged")

        work = _run_dir() / "transform"
        work.mkdir(exist_ok=True)

        src = hook.download_file(
            prefix + key, bucket, str(work), preserve_file_name=True,
            use_autogenerated_subdir=False,
        )  # fmt: skip

        source_hash = md5_file(src)
        dst = str(work / "staging.parquet")
        df = readers.parse(dataset.reader, src)
        stats = write_text_parquet(df, dst, FILE_NAME, source_hash, load_id_of(key))

        return staging_record(run_id, key, staging_key=staging_key(key), status="converted",
                              source_hash=source_hash, local_path=dst, **stats)  # fmt: skip

    @task
    def verify_structural_data_drift(rec: dict) -> dict:
        """Colunas e volume contra o contrato; `quarantined` não entra na staging."""
        if rec["status"] != "converted":
            return rec

        datasets = contract.load()
        dataset = contract.match(rec["raw_key"], datasets)
        previous = previous_rows(PostgresHook(postgres_conn_id="postgres_default"))
        meta = pq.read_metadata(rec["local_path"])

        header = [c for c in meta.schema.names if not c.startswith("_")]  # linhagem fora

        status, details = contract.structural_status(
            header, dataset, meta.num_rows, previous(dataset, datasets)
        )
        rec["structural"] = status

        if status == "quarantined":
            rec.update(status="quarantined", error_message=json.dumps(details)[:500])
        elif status in ("additive_drift", "known_rename"):
            rec["error_message"] = json.dumps(details)[:500]

        return rec

    @task(retries=2, retry_exponential_backoff=True)
    @fail_fast_on_missing_config
    def upload_staging(rec: dict) -> dict:
        """O Parquet aprovado em staging/fgv/incc_m/load_id=<id>/ e o lake._staging_log."""
        hook, bucket, prefix = _lake()
        if rec["status"] == "converted":
            hook.load_file(rec["local_path"], prefix + rec["staging_key"], bucket, replace=True)
        if not prefix:  # o dry run lê as tabelas de controle, mas não grava
            record(PostgresHook(postgres_conn_id="postgres_default"), [rec])
        return {k: rec.get(k) for k in ("raw_key", "status", "structural", "error_message")}

    @task(trigger_rule="none_failed")
    def report() -> None:
        """Resumo da execução; falha com erro ou quarentena, fica verde sem carga nova."""
        rec = get_current_context()["ti"].xcom_pull(task_ids="upload_staging")
        if not rec:  # upload_to_s3 pulou: a fonte não mudou
            logging.info("[incc_m_ingest_dag] sem carga nova: o INCC-M não mudou")
            return
        body, failed, notable = summary({"keys": [rec["raw_key"]]}, [rec])
        (logging.error if failed else logging.warning if notable else logging.info)(
            "[incc_m_ingest_dag] report\n%s", body
        )
        if failed:
            raise AirflowFailException("ingestão do INCC-M com erro ou quarentena; veja o resumo no log")

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        shutil.rmtree(_run_dir(), ignore_errors=True)

    staged = upload_staging(verify_structural_data_drift(transform(upload_to_s3(extract()))))
    staged >> report() >> cleanup()


dag_instance = incc_m_ingest_dag()
