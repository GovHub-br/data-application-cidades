"""Transformação dedicada para os cortes volumosos e já restritos do CadÚnico.

Mantém a atualização cotidiana SFTP -> MinIO -> painel livre de arquivos de dezenas
de GB. O fluxo é idempotente por hash e nunca publica CPF, NIS, nome ou endereço em
staging sem passar pelo mascaramento.
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PADRAO_CADUNICO = "ARQ_PESSOA_PBF_12122025_SB8.TXT,ARQ_FAMILIA_PBF_12122025_SB8.TXT"


@dag(
    # É chamada no encadeamento diário após a transformação leve do lake.
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Juan Pablo",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    params={"dry_run": False, "pattern": PADRAO_CADUNICO, "chunksize": 100_000},
    tags=["minio", "cadunico", "mascaramento", "staging", "cidades"],
)
def cadunico_transform_dag() -> None:
    """Mascara e converte CadÚnico em janela dedicada, sem PII na staging."""

    @task
    def copiar_cadunico_para_mascarado() -> None:
        from copiar_raw_para_mascarado import run as run_copy

        params = get_current_context()["params"]
        resultado = run_copy(pattern=str(params.get("pattern") or PADRAO_CADUNICO))
        logging.info("[cadunico_transform_dag] raw->masked: %s", resultado)

    @task
    def mascarar_pii_cadunico() -> None:
        from mascarar_minio import run as run_mask

        params = get_current_context()["params"]
        resultado = run_mask(
            apply=not bool(params.get("dry_run", False)),
            pattern=str(params.get("pattern") or PADRAO_CADUNICO), prefix="masked/"
        )
        logging.info("[cadunico_transform_dag] mascaramento: %s", resultado)

    @task
    def converter_cadunico_para_staging() -> None:
        from raw_para_staging import run as run_staging

        params = get_current_context()["params"]
        resultado = run_staging(
            apply=not bool(params.get("dry_run", False)),
            pattern=str(params.get("pattern") or PADRAO_CADUNICO),
            chunksize=int(params.get("chunksize") or 100_000),
            bad_lines="skip", prefix="masked/"
        )
        logging.info("[cadunico_transform_dag] staging: %s", resultado)

    copia = copiar_cadunico_para_mascarado()
    mascaramento = mascarar_pii_cadunico()
    staging = converter_cadunico_para_staging()
    copia >> mascaramento >> staging


dag_instance = cadunico_transform_dag()
