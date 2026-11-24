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
from airflow.sdk.exceptions import AirflowFailException

from plugins.datalake import contract, readers
from plugins.datalake.api import staging_key, staging_record, write_text_parquet
from plugins.datalake.keys import RAW_PREFIX, load_id, load_id_of, raw_key
from plugins.datalake.tasks import previous_rows, record, summary
from scripts.lake_utils import md5_file


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


def _endpoint_and_params(config: dict) -> tuple[str, dict]:
    """Uma entrada do IBGE_CONFIGURACOES -> caminho e parâmetros da API de agregados v3."""
    endpoint = (
        f"api/v3/agregados/{config['agregado']}/periodos/{config.get('periodos', '-20')}"
        f"/variaveis/{config['variaveis']}"
    )
    params = {"localidades": "N1[1]"}  # Brasil
    if config.get("classificacao_id") is not None and config.get("categoria") is not None:
        # a API separa variáveis por `|` e categorias por `,`; aceita os dois na Variable
        categories = str(config["categoria"]).replace("|", ",")
        params["classificacao"] = f"{config['classificacao_id']}[{categories}]"
    return endpoint, params


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Mateus", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"dry_run": False},
    tags=["ibge", "pib_construcao", "sinapi", "conjuntura", "data-lake", "ingest"],
)
def ibge_ingest_dag() -> None:  # noqa: C901 - as tasks da DAG ficam dentro dela
    """Agregados do IBGE (API v3) listados no IBGE_CONFIGURACOES para o lake (ADR-0004).

    Uma tabela por entrada da Variable, cada uma em raw/ibge/<tabela>/load_id=<id>/ com a
    resposta JSON como a API entrega. Cada task percorre as tabelas: uma tabela que falha
    na API não impede as outras, e o report falha a execução no fim.

    Param `dry_run`: grava tudo embaixo de `dryrun/` e não escreve nas tabelas de controle.
    """

    @task(retries=2, retry_exponential_backoff=True)
    def extract() -> dict:
        """A resposta de cada agregado em disco; os erros seguem para o report."""
        configs = Variable.get("IBGE_CONFIGURACOES", deserialize_json=True)
        if not configs:
            raise AirflowFailException("IBGE_CONFIGURACOES vazia: nenhuma tabela para ingerir")
        hook = HttpHook(method="GET", http_conn_id="ibge")
        files, errors = {}, []
        for config in configs:
            table = config["tabela"]
            endpoint, params = _endpoint_and_params(config)
            try:
                response = hook.run(
                    endpoint, data=params, extra_options={"timeout": 120}
                )  # != 2xx levanta
                if not isinstance(response.json(), list) or not response.json():
                    raise ValueError("resposta sem dados")
            except Exception as exc:  # noqa: BLE001 - uma tabela não derruba as outras
                errors.append(f"{table}: {exc}"[:300])
                continue
            path = _run_dir() / f"{table}.json"
            path.write_bytes(response.content)
            files[table] = str(path)
        if not files:
            raise AirflowFailException(f"nenhuma tabela do IBGE respondeu: {errors}")
        return {"files": files, "errors": errors}

    @task(retries=2, retry_exponential_backoff=True)
    def upload_to_s3(extracted: dict) -> list[str]:
        """Cada JSON em raw/ibge/<tabela>/load_id=<id>/; a tabela igual à última carga não pousa.

        O md5 vai no metadado do objeto, e a próxima execução compara com ele (ADR-0005).
        """
        hook, bucket, prefix = _lake()
        load = load_id(get_current_context()["dag_run"].run_after)  # o mesmo num retry
        client, keys = hook.get_conn(), []
        for table, path in extracted["files"].items():
            folder, file_name, md5 = f"ibge/{table}", f"{table}.json", md5_file(path)
            previous_loads = sorted(
                {
                    load_id_of(k)
                    for k in hook.list_keys(bucket, f"{prefix}{RAW_PREFIX}{folder}/") or []
                }
                - {load, None}
            )
            if previous_loads:
                previous_key = f"{prefix}{raw_key(folder, file_name, previous_loads[-1])}"
                head = client.head_object(Bucket=bucket, Key=previous_key)
                if head["Metadata"].get("md5") == md5:
                    logging.info("[ibge_ingest_dag] %s não mudou desde a última carga", table)
                    continue
            key = raw_key(folder, file_name, load)
            client.upload_file(path, bucket, prefix + key, ExtraArgs={"Metadata": {"md5": md5}})
            keys.append(key)
        return keys

    @task
    def transform(keys: list[str]) -> list[dict]:
        """Cada raw em Parquet local (texto + linhagem), pelo leitor do contrato `ibge_agregados` (contracts/snh/ibge/agregados.odcs.yaml)."""
        hook, bucket, prefix = _lake()
        run_id, datasets, out = get_current_context()["run_id"], contract.load(), []
        for i, key in enumerate(keys):
            dataset = contract.match(key, datasets)
            if dataset is None:  # tabela nova na Variable sem contrato: fica na raw
                out.append(staging_record(run_id, key, status="uncataloged"))
                continue
            work = _run_dir() / f"transform_{i}"
            work.mkdir(exist_ok=True)
            try:
                src = hook.download_file(
                    prefix + key, bucket, str(work), preserve_file_name=True,
                    use_autogenerated_subdir=False,
                )  # fmt: skip
                source_hash = md5_file(src)
                dst = str(work / "staging.parquet")
                df = readers.parse(dataset.reader, src)
                stats = write_text_parquet(df, dst, Path(key).name, source_hash, load_id_of(key))
            except Exception as exc:  # noqa: BLE001 - uma tabela não derruba as outras
                out.append(staging_record(run_id, key, status="error", error_message=str(exc)))
                continue
            out.append(staging_record(run_id, key, staging_key=staging_key(key),
                                      status="converted", source_hash=source_hash,
                                      local_path=dst, **stats))  # fmt: skip
        return out

    @task
    def verify_structural_data_drift(recs: list[dict]) -> list[dict]:
        """Colunas e volume de cada tabela contra o contrato; `quarantined` fica fora."""
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
        """Os Parquet aprovados em staging/ibge/<tabela>/load_id=<id>/ e o lake._staging_log."""
        hook, bucket, prefix = _lake()
        for rec in recs:
            if rec["status"] == "converted":
                hook.load_file(rec["local_path"], prefix + rec["staging_key"], bucket, replace=True)
        if recs and not prefix:  # o dry run lê as tabelas de controle, mas não grava
            record(PostgresHook(postgres_conn_id="postgres_default"), recs)
        return [
            {k: r.get(k) for k in ("raw_key", "status", "structural", "error_message")}
            for r in recs
        ]

    @task(trigger_rule="none_failed")
    def report() -> None:
        """Resumo das tabelas; falha com erro de API, conversão ou quarentena."""
        ti = get_current_context()["ti"]
        errors = (ti.xcom_pull(task_ids="extract") or {}).get("errors") or []
        recs = ti.xcom_pull(task_ids="upload_staging") or []
        body, failed, notable = summary(
            {"keys": [r["raw_key"] for r in recs], "errors": errors}, recs
        )
        (logging.error if failed else logging.warning if notable else logging.info)(
            "[ibge_ingest_dag] report\n%s", body
        )
        if failed:
            raise RuntimeError("ingestão do IBGE com erro ou quarentena; veja o resumo no log")

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        shutil.rmtree(_run_dir(), ignore_errors=True)

    staged = upload_staging(verify_structural_data_drift(transform(upload_to_s3(extract()))))
    staged >> report() >> cleanup()


dag_instance = ibge_ingest_dag()
