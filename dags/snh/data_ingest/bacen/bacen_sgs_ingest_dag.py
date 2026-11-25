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

# snh/raw/bacen/financiamentos_imobiliarios/load_id=<id>/<série>.json, uma por série da Variable
FOLDER = "bacen/financiamentos_imobiliarios"


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
    default_args={"owner": "Mateus", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False},
    tags=["bacen", "sgs", "financiamento_imobiliario", "conjuntura", "data-lake", "ingest"],
)
def bacen_sgs_ingest_dag() -> None:  # noqa: C901 - as tasks da DAG ficam dentro dela
    """Séries do SGS do Banco Central listadas no BACEN_SERIES para o lake (ADR-0004).

    Uma carga é o conjunto das séries: cada uma em
    raw/bacen/financiamentos_imobiliarios/load_id=<id>/<série>.json, como a API entrega.
    Se uma série falha, nada pousa: a prata usa a última carga, que precisa estar inteira.

    Param `dry_run`: grava tudo embaixo de `dryrun/` e não escreve nas tabelas de controle.
    """

    @task(retries=2, retry_exponential_backoff=True, execution_timeout=timedelta(minutes=30))
    @fail_fast_on_missing_config
    def extract() -> list[str]:
        """Cada série do BACEN_SERIES (nome -> código), lida na task, em disco.

        O SGS ignora `ultimos` e devolve a série inteira, e às vezes responde 200 com uma
        página HTML: só passa resposta que é uma lista JSON.
        """
        series = Variable.get("BACEN_SERIES", deserialize_json=True)
        if isinstance(series, list):  # formato antigo da Variable: [{nome: código}]
            series = series[0] if series else {}
        if not series:
            raise AirflowFailException("BACEN_SERIES vazia: nenhuma série para ingerir")
        hook, paths = HttpHook(method="GET", http_conn_id="bacen_sgs"), []
        for name, code in series.items():
            response = hook.run(
                f"dados/serie/bcdata.sgs.{code}/dados",
                data={"formato": "json", "ultimos": 13},
                extra_options={"timeout": 120},
            )  # != 2xx levanta
            try:
                rows = response.json()
            except ValueError:
                rows = None
            if not isinstance(rows, list) or not rows:
                raise AirflowFailException(f"o SGS não devolveu a série {name} ({code})")
            path = _run_dir() / f"{name}.json"
            path.write_bytes(response.content)
            paths.append(str(path))
        return paths

    @task(retries=2, retry_exponential_backoff=True)
    @fail_fast_on_missing_config
    def upload_to_s3(paths: list[str]) -> list[str]:
        """As séries na mesma pasta load_id=; se nenhuma mudou desde a última carga, nada pousa.

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
                raise AirflowSkipException("nenhuma série do SGS mudou desde a última carga")
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
        """Cada série em Parquet local (texto + linhagem), pelo leitor do contrato `bacen_credito_imobiliario` (contracts/snh/bacen/)."""
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
        """Colunas e volume de cada série contra o contrato; `quarantined` fica fora."""
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
        """As séries aprovadas em staging/bacen/financiamentos_imobiliarios/load_id=<id>/."""
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
        """Resumo das séries; falha com erro ou quarentena, fica verde sem carga nova."""
        recs = get_current_context()["ti"].xcom_pull(task_ids="upload_staging")
        if not recs:  # upload_to_s3 pulou: nenhuma série mudou
            logging.info("[bacen_sgs_ingest_dag] sem carga nova: o SGS não mudou")
            return
        body, failed, notable = summary({"keys": [r["raw_key"] for r in recs]}, recs)
        (logging.error if failed else logging.warning if notable else logging.info)(
            "[bacen_sgs_ingest_dag] report\n%s", body
        )
        if failed:
            raise AirflowFailException("ingestão do SGS com erro ou quarentena; veja o resumo no log")

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        shutil.rmtree(_run_dir(), ignore_errors=True)

    staged = upload_staging(verify_structural_data_drift(transform(upload_to_s3(extract()))))
    staged >> report() >> cleanup()


dag_instance = bacen_sgs_ingest_dag()
