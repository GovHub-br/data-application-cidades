import ftplib
import json
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, get_current_context, task, teardown
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from plugins.datalake import contract, readers
from plugins.datalake.api import staging_key, staging_record, write_text_parquet
from plugins.datalake.config import fail_fast_on_missing_config
from plugins.datalake.keys import RAW_PREFIX, load_id, load_id_of, raw_key
from plugins.datalake.tasks import previous_rows, record, summary
from scripts.lake_utils import md5_file

# FTP público do PDET (sem login): <BASE>/<ano>/<ano><mês>/CAGED{MOV,FOR,EXC}<ano><mês>.7z
FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE = "/pdet/microdados/NOVO CAGED"
FIRST_YEAR = 2024  # início da série que o boletim usa (o estoque se ancora em jan/2024)
FILE = re.compile(r"CAGED(MOV|FOR|EXC)(\d{6})\.7z")
# snh/raw/novo_caged/pdet/<ano>/<ano><mês>/load_id=<id>/<arquivo>.7z, espelho do FTP
FOLDER = "novo_caged/pdet"


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


def _folder(name: str) -> str:
    """CAGEDMOV202606.7z -> novo_caged/pdet/2026/202606 (a pasta do FTP)."""
    period = FILE.fullmatch(name).group(2)
    return f"{FOLDER}/{period[:4]}/{period}"


@dag(
    schedule="0 9 * * *",  # o PDET publica junto com a divulgação mensal, sem dia fixo
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Lucas Bottino", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False, "limit": 0},
    tags=["cidades", "novo_caged", "construcao", "data-lake", "ingest"],
)
def novo_caged_pdet_ingest_dag() -> None:  # noqa: C901 - as tasks da DAG ficam dentro dela
    """Microdados do Novo CAGED (PDET/MTE) para o lake (ADR-0004), no lugar do painel Power BI.

    extract → upload_to_s3 → transform → verify_structural_data_drift → upload_staging
    → report, e cleanup no fim, um arquivo por vez. Cada mês publicado traz três arquivos
    (MOV, FOR, EXC) que não mudam depois: só os que ainda não estão na raw são baixados.
    A staging guarda as movimentações da construção (seção F); o saldo e o estoque por
    divisão saem na prata do conjuntura_dbt.

    Params: `dry_run` grava tudo embaixo de `dryrun/` e não escreve nas tabelas de
    controle; `limit` baixa no máximo N arquivos (0 = todos os que faltam).
    """

    @task(retries=2, retry_exponential_backoff=True, execution_timeout=timedelta(hours=3))
    @fail_fast_on_missing_config
    def extract() -> list[str]:
        """Os arquivos do FTP que ainda não estão na raw, em disco."""
        hook, bucket, prefix = _lake()
        landed = {
            k.rsplit("/", 1)[-1]
            for k in hook.list_keys(bucket, f"{prefix}{RAW_PREFIX}{FOLDER}/") or []
        }
        limit = int(get_current_context()["params"]["limit"] or 0)
        paths = []
        with ftplib.FTP(FTP_HOST, timeout=120) as ftp:  # FTP anônimo
            ftp.login()
            ftp.encoding = "latin-1"

            def children(path: str) -> list[str]:  # o NLST devolve nome solto ou caminho
                return sorted(n.rsplit("/", 1)[-1] for n in ftp.nlst(path))

            for year in children(FTP_BASE):
                if not year.isdigit() or int(year) < FIRST_YEAR:
                    continue
                for month in children(f"{FTP_BASE}/{year}"):
                    for name in children(f"{FTP_BASE}/{year}/{month}"):
                        if not FILE.fullmatch(name) or name in landed:
                            continue
                        local = _run_dir() / name
                        with open(local, "wb") as f:
                            ftp.retrbinary(f"RETR {FTP_BASE}/{year}/{month}/{name}", f.write)
                        paths.append(str(local))
                        if limit and len(paths) >= limit:
                            return paths
        if not paths:
            raise AirflowSkipException("nenhum arquivo novo do Novo CAGED no FTP do PDET")
        logging.info("[novo_caged_pdet_ingest_dag] %d arquivo(s) novo(s)", len(paths))
        return paths

    @task(retries=2, retry_exponential_backoff=True)
    @fail_fast_on_missing_config
    def upload_to_s3(path: str) -> str:
        """O 7z como veio em raw/novo_caged/pdet/<ano>/<ano><mês>/load_id=<id>/ (md5 no metadado)."""
        hook, bucket, prefix = _lake()
        load = load_id(get_current_context()["dag_run"].run_after)  # o mesmo num retry
        name = Path(path).name
        key = raw_key(_folder(name), name, load)
        hook.get_conn().upload_file(
            path, bucket, prefix + key, ExtraArgs={"Metadata": {"md5": md5_file(path)}}
        )
        return key

    @task(pool="lake_conversao")
    def transform(key: str) -> dict:
        """A raw em Parquet local (texto + linhagem), pelo leitor do contrato `novo_caged_pdet` (contracts/snh/novo_caged/pdet.odcs.yaml)."""
        hook, bucket, prefix = _lake()
        run_id = get_current_context()["run_id"]
        dataset = contract.match(key, contract.load())
        if dataset is None:  # fica na raw e vai para o resumo (ADR-0006)
            return staging_record(run_id, key, status="uncataloged")
        work = _run_dir() / "transform" / Path(key).stem
        work.mkdir(parents=True, exist_ok=True)
        src = hook.download_file(
            prefix + key, bucket, str(work), preserve_file_name=True,
            use_autogenerated_subdir=False,
        )  # fmt: skip
        source_hash = md5_file(src)
        dst = str(work / "staging.parquet")
        df = readers.parse(dataset.reader, src)
        stats = write_text_parquet(df, dst, Path(key).name, source_hash, load_id_of(key))
        Path(src).unlink()  # o 7z já está na raw; o disco do worker é pouco
        return staging_record(run_id, key, staging_key=staging_key(key), status="converted",
                              source_hash=source_hash, local_path=dst, **stats)  # fmt: skip

    @task
    def verify_structural_data_drift(rec: dict) -> dict:
        """Colunas contra o contrato; `quarantined` não entra na staging."""
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
        """O Parquet aprovado em staging/novo_caged/pdet/<ano>/<ano><mês>/load_id=<id>/ e o lake._staging_log."""
        hook, bucket, prefix = _lake()
        if rec["status"] == "converted":
            hook.load_file(rec["local_path"], prefix + rec["staging_key"], bucket, replace=True)
        if not prefix:  # o dry run lê as tabelas de controle, mas não grava
            record(PostgresHook(postgres_conn_id="postgres_default"), [rec])
        return {k: rec.get(k) for k in ("raw_key", "status", "structural", "error_message")}

    @task(trigger_rule="none_failed")
    def report(recs: list) -> None:
        """Resumo da execução; falha com erro ou quarentena, fica verde sem arquivo novo."""
        recs = list(recs or [])
        if not recs:
            logging.info("[novo_caged_pdet_ingest_dag] sem arquivo novo no FTP do PDET")
            return
        body, failed, notable = summary({"keys": [r["raw_key"] for r in recs]}, recs)
        (logging.error if failed else logging.warning if notable else logging.info)(
            "[novo_caged_pdet_ingest_dag] report\n%s", body
        )
        if failed:
            raise AirflowFailException(
                "ingestão do Novo CAGED (PDET) com erro ou quarentena; veja o resumo no log"
            )

    @teardown  # roda sempre e não conta para o estado da execução
    def cleanup() -> None:
        shutil.rmtree(_run_dir(), ignore_errors=True)

    keys = upload_to_s3.expand(path=extract())
    staged = upload_staging.expand(
        rec=verify_structural_data_drift.expand(rec=transform.expand(key=keys))
    )
    report(staged) >> cleanup()


dag_instance = novo_caged_pdet_ingest_dag()
