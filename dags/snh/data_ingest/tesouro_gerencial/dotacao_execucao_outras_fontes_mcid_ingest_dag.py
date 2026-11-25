import json
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from imap_tools import AND, MailBox

from plugins.datalake.config import fail_fast_on_missing_config
from plugins.datalake import contract, readers
from plugins.datalake.api import staging_key, staging_record, write_text_parquet
from plugins.datalake.keys import RAW_PREFIX, load_id, load_id_of, raw_key
from plugins.datalake.tasks import previous_rows, record, summary
from scripts.lake_utils import md5_file

# snh/raw/siafi-tesouro-gerencial/dotacao_execucao_outras_fontes_mcid/load_id=<id>/<nn>_<anexo>.zip
FOLDER = "siafi-tesouro-gerencial/dotacao_execucao_outras_fontes_mcid"
SUBJECT = "dotacao_execucao_outras_fontes_mcid"


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
    dag_id="dotacao_execucao_outras_fontes_mcid_ingest_dag",
    schedule="0 12 * * *",  # 09h em Brasília: o e-mail chega entre 04h e 08h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Lucas", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False},
    tags=["email", "mcid", "tesouro", "dotacao", "execucao", "data-lake", "ingest"],
)
def dotacao_execucao_outras_fontes_mcid_ingest_dag() -> None:  # noqa: C901 - as tasks da DAG ficam dentro dela
    """Dotação e execução orçamentária do MCID (Tesouro Gerencial, por e-mail) para o lake.

    Uma carga são os anexos ZIP do e-mail do dia, como chegaram, em
    raw/siafi-tesouro-gerencial/dotacao_execucao_outras_fontes_mcid/load_id=<id>/. Cada
    carga é o relatório inteiro: a prata fica com a última (ADR-0004).

    Param `dry_run`: grava tudo embaixo de `dryrun/` e não escreve nas tabelas de controle.
    """

    @task(retries=2, retry_exponential_backoff=True, execution_timeout=timedelta(minutes=30))
    @fail_fast_on_missing_config
    def extract() -> list[str]:
        """Os anexos ZIP do e-mail do Tesouro Gerencial do dia da execução, como chegaram.

        A caixa vem da Connection `tesouro_imap` (servidor IMAP no host, login e senha;
        o remetente esperado em `sender_email` no extra). O e-mail chega de terça a sábado
        de manhã: sem e-mail no dia, a execução é pulada. Reexecutar um dia passado busca
        o e-mail daquele dia.
        """
        conn = BaseHook.get_connection(conn_id="tesouro_imap")
        dag_run = get_current_context()["dag_run"]
        # o dia da execução: a data lógica, que se escolhe ao disparar para um dia passado
        moment = dag_run.logical_date or dag_run.run_after
        day = moment.astimezone(ZoneInfo("America/Sao_Paulo")).date()
        criteria = AND(date=day, from_=conn.extra_dejson["sender_email"], subject=SUBJECT)
        paths = []
        with MailBox(conn.host).login(conn.login, conn.password) as mailbox:
            for message in mailbox.fetch(criteria, bulk=True):  # um FETCH só (evita overquota)
                for attachment in message.attachments:
                    if attachment.filename.lower().endswith(".zip"):
                        name = re.sub(r"[^A-Za-z0-9_.-]", "_", attachment.filename)
                        path = _run_dir() / f"{len(paths) + 1:02d}_{name}"
                        path.write_bytes(attachment.payload)
                        paths.append(str(path))
        if not paths:
            raise AirflowSkipException(f"nenhum e-mail {SUBJECT!r} com ZIP em {day}")
        return paths

    @task(retries=2, retry_exponential_backoff=True)
    @fail_fast_on_missing_config
    def upload_to_s3(paths: list[str]) -> list[str]:
        """Os anexos na mesma pasta load_id=; se são iguais aos da última carga, nada pousa.

        O md5 vai no metadado de cada objeto, e a próxima execução compara com ele.
        """
        hook, bucket, prefix = _lake()
        load = load_id(get_current_context()["dag_run"].run_after)  # o mesmo num retry
        client = hook.get_conn()
        hashes = {Path(p).name: md5_file(p) for p in paths}
        previous_loads = sorted(
            {load_id_of(k) for k in hook.list_keys(bucket, f"{prefix}{RAW_PREFIX}{FOLDER}/") or []}
            - {load, None}
        )
        if previous_loads:
            base = f"{prefix}raw/{FOLDER}/load_id={previous_loads[-1]}/"
            previous = {
                k.rsplit("/", 1)[-1]: client.head_object(Bucket=bucket, Key=k)["Metadata"].get("md5")
                for k in hook.list_keys(bucket, base) or []
            }  # fmt: skip
            if previous == hashes:
                raise AirflowSkipException("os anexos são iguais aos da última carga")
        keys = []
        for path in paths:
            key = raw_key(FOLDER, Path(path).name, load)
            client.upload_file(
                path, bucket, prefix + key, ExtraArgs={"Metadata": {"md5": hashes[Path(path).name]}}
            )
            keys.append(key)
        return keys

    @task
    def transform(keys: list[str]) -> list[dict]:
        """Cada anexo em Parquet local (texto + linhagem), pelo leitor do contrato `tesouro_dotacao_execucao` (contracts/snh/tesouro/)."""
        hook, bucket, prefix = _lake()
        run_id, datasets, out = get_current_context()["run_id"], contract.load(), []
        for i, key in enumerate(keys):
            dataset = contract.match(key, datasets)
            if dataset is None:  # fica na raw e vai para o resumo (ADR-0006)
                out.append(staging_record(run_id, key, status="uncataloged"))
                continue
            work = _run_dir() / f"transform_{i}"
            work.mkdir(exist_ok=True)
            src = hook.download_file(
                prefix + key, bucket, str(work), preserve_file_name=True,
                use_autogenerated_subdir=False,
            )  # fmt: skip
            source_hash = md5_file(src)
            dst = str(work / "staging.parquet")
            df = readers.parse(dataset.reader, src)
            stats = write_text_parquet(df, dst, Path(key).name, source_hash, load_id_of(key))
            out.append(staging_record(run_id, key, staging_key=staging_key(key),
                                      status="converted", source_hash=source_hash,
                                      local_path=dst, **stats))  # fmt: skip
        return out

    @task
    def verify_structural_data_drift(recs: list[dict]) -> list[dict]:
        """Colunas e volume de cada anexo contra o contrato; `quarantined` fica fora."""
        datasets = contract.load()
        previous = previous_rows(PostgresHook(postgres_conn_id="postgres_default"))
        for rec in recs:
            if rec["status"] != "converted":
                continue
            dataset = contract.match(rec["raw_key"], datasets)
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
        return recs

    @task(retries=2, retry_exponential_backoff=True)
    @fail_fast_on_missing_config
    def upload_staging(recs: list[dict]) -> list[dict]:
        """Os anexos aprovados em staging/siafi-tesouro-gerencial/dotacao_execucao_outras_fontes_mcid/load_id=<id>/."""
        hook, bucket, prefix = _lake()
        for rec in recs:
            if rec["status"] == "converted":
                hook.load_file(rec["local_path"], prefix + rec["staging_key"], bucket, replace=True)
        if not prefix:  # o dry run lê as tabelas de controle, mas não grava
            record(PostgresHook(postgres_conn_id="postgres_default"), recs)
        return [
            {k: r.get(k) for k in ("raw_key", "status", "structural", "error_message")}
            for r in recs
        ]

    @task(trigger_rule="none_failed")
    def report() -> None:
        """Resumo dos anexos; falha com erro ou quarentena, fica verde sem carga nova."""
        recs = get_current_context()["ti"].xcom_pull(task_ids="upload_staging")
        if not recs:  # extract ou upload_to_s3 pulou: sem e-mail ou sem mudança
            logging.info(
                "[dotacao_execucao_outras_fontes_mcid_ingest_dag] sem carga nova: o relatório não mudou"
            )
            return
        body, failed, notable = summary({"keys": [r["raw_key"] for r in recs]}, recs)
        (logging.error if failed else logging.warning if notable else logging.info)(
            "[dotacao_execucao_outras_fontes_mcid_ingest_dag] report\n%s", body
        )
        if failed:
            raise AirflowFailException(
                "ingestão do Tesouro com erro ou quarentena; veja o resumo no log"
            )

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        shutil.rmtree(_run_dir(), ignore_errors=True)

    staged = upload_staging(verify_structural_data_drift(transform(upload_to_s3(extract()))))
    staged >> report() >> cleanup()


dag_instance = dotacao_execucao_outras_fontes_mcid_ingest_dag()
