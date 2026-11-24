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

from plugins.datalake import contract, readers
from plugins.datalake.readers import SourceLayoutChanged, novo_caged_values
from plugins.datalake.api import staging_key, staging_record, write_text_parquet
from plugins.datalake.keys import load_id, load_id_of, raw_key
from plugins.datalake.tasks import previous_rows, record, summary
from plugins.novo_caged_query import ENDPOINT, expected_periods, query_payload
from scripts.lake_utils import md5_file

# raw/novo_caged/saldo_estoque_construcao_edificios/load_id=<id>/<ano>-<mês>.json, um por mês
FOLDER = "novo_caged/saldo_estoque_construcao_edificios"
CNAE_DIVISION = "Construção de Edifícios"  # None = toda a construção


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
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Milena Rocha", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False},
    tags=["cidades", "novo_caged", "construcao", "saldo", "estoque", "data-lake", "ingest"],
)
def novo_caged_construcao_edificios() -> None:  # noqa: C901 - as tasks da DAG ficam dentro dela
    """Saldo e estoque de empregos na construção de edifícios (Novo CAGED) para o lake (ADR-0004).

    Uma carga é a série desde jan/2024, um arquivo por mês em
    raw/novo_caged/saldo_estoque_construcao_edificios/load_id=<id>/, como o Power BI responde. Cada carga é completa:
    a prata fica com a última.

    Param `dry_run`: grava tudo embaixo de `dryrun/` e não escreve nas tabelas de controle.
    """

    @task(retries=2, retry_exponential_backoff=True)
    def extract() -> list[str]:
        """Cada mês desde jan/2024, como o Power BI responde (Connection `powerbi_caged`).

        O ano e o mês não vêm na resposta: vão no nome do arquivo (`<ano>-<mês>.json`). Só
        o mês mais recente pode faltar (o CAGED publica o mês m no fim de m+1); qualquer
        outro mês vazio falha antes de pousar uma carga incompleta.
        """
        hook = HttpHook(method="POST", http_conn_id="powerbi_caged")
        periods = expected_periods(get_current_context()["dag_run"].run_after.date())
        paths, missing = [], []
        for year, month in periods:
            response = hook.run(
                ENDPOINT,
                json=query_payload(year, month, CNAE_DIVISION),
                extra_options={"timeout": 120},
            )
            try:
                novo_caged_values(response.json())
            except (ValueError, SourceLayoutChanged):
                missing.append((year, month))
                continue
            path = _run_dir() / f"{year}-{month:02d}.json"
            path.write_bytes(response.content)
            paths.append(str(path))
        if missing and missing[-1] == periods[-1]:
            logging.warning(
                "[novo_caged_construcao_edificios] %d-%02d ainda não publicado", *missing.pop()
            )
        if missing:
            raise AirflowFailException(f"meses do Novo CAGED sem dado: {missing}")
        return paths

    @task(retries=2, retry_exponential_backoff=True)
    def upload_to_s3(paths: list[str]) -> list[str]:
        """Os meses na mesma pasta load_id=; se nenhum mudou desde a última carga, nada pousa.

        O md5 vai no metadado de cada objeto, e a próxima execução compara com ele.
        """
        hook, bucket, prefix = _lake()
        load = load_id(get_current_context()["dag_run"].run_after)  # o mesmo num retry
        client = hook.get_conn()
        hashes = {Path(p).name: md5_file(p) for p in paths}
        previous_loads = sorted(
            {load_id_of(k) for k in hook.list_keys(bucket, f"{prefix}raw/{FOLDER}/") or []}
            - {load, None}
        )
        if previous_loads:
            base = f"{prefix}raw/{FOLDER}/load_id={previous_loads[-1]}/"
            previous = {
                k.rsplit("/", 1)[-1]: client.head_object(Bucket=bucket, Key=k)["Metadata"].get("md5")
                for k in hook.list_keys(bucket, base) or []
            }  # fmt: skip
            if previous == hashes:
                raise AirflowSkipException("nenhum mês do Novo CAGED mudou desde a última carga")
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
        """Cada mês em Parquet local (texto + linhagem), pelo leitor do contrato `novo_caged_construcao` (contracts/novo_caged/)."""
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
        """Colunas e volume de cada mês contra o contrato; `quarantined` fica fora."""
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
    def upload_staging(recs: list[dict]) -> list[dict]:
        """Os meses aprovados em staging/novo_caged/saldo_estoque_construcao_edificios/load_id=<id>/."""
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
        """Resumo dos meses; falha com erro ou quarentena, fica verde sem carga nova."""
        recs = get_current_context()["ti"].xcom_pull(task_ids="upload_staging")
        if not recs:  # upload_to_s3 pulou: nenhum mês mudou
            logging.info("[novo_caged_construcao_edificios] sem carga nova: o Novo CAGED não mudou")
            return
        body, failed, notable = summary({"keys": [r["raw_key"] for r in recs]}, recs)
        (logging.error if failed else logging.warning if notable else logging.info)(
            "[novo_caged_construcao_edificios] report\n%s", body
        )
        if failed:
            raise RuntimeError(
                "ingestão do Novo CAGED com erro ou quarentena; veja o resumo no log"
            )

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        shutil.rmtree(_run_dir(), ignore_errors=True)

    staged = upload_staging(verify_structural_data_drift(transform(upload_to_s3(extract()))))
    staged >> report() >> cleanup()


dag_instance = novo_caged_construcao_edificios()
