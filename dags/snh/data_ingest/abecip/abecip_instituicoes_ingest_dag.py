"""Consolida as competências da ABECIP por instituição financeira em staging.

O relatório mensal da ABECIP traz a abertura do financiamento imobiliário por
instituição — a tabela que o boletim publica na seção de crédito. Ela é
extraída por outro time, que grava uma competência por diretório em
`snh/raw/abecip/<AAAA-MM>/financiamentos_por_instituicao.json`.

A DAG lê TODAS as competências presentes no raw e reescreve o parquet inteiro.
Reescrever é de propósito: a ABECIP revisa competências já publicadas, e uma
carga incremental preservaria o número velho ao lado do corrigido.

Não busca nada na internet: consome o que o pipeline de extração já depositou. A
staging sai pelo mesmo escritor das outras ingestões (texto + linhagem, ADR-0002).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, get_current_context, task

from plugins.datalake.api import write_text_parquet
from plugins.datalake.keys import RAW_PREFIX, STAGING_PREFIX, load_id

PREFIX = f"{RAW_PREFIX}abecip/"
DATASET = "financiamentos_por_instituicao"
STAGING_KEY = f"{STAGING_PREFIX}abecip/{DATASET}.parquet"
RAW_KEY = re.compile(rf"^{PREFIX}(\d{{4}}-\d{{2}})/{DATASET}\.json$")


@dag(
    dag_id="abecip_instituicoes_ingest_dag",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Lucas Bottino", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["conjuntura", "abecip", "ingestao"],
    description=(
        "Consolida snh/raw/abecip/<competência>/financiamentos_por_instituicao.json "
        "em snh/staging/abecip/financiamentos_por_instituicao.parquet"
    ),
)
def abecip_instituicoes_ingest_dag() -> None:
    @task
    def consolidar() -> dict:
        hook, bucket = S3Hook(aws_conn_id="lake_mcid"), Variable.get("lake_bucket")
        competencias = {
            m.group(1): key
            for key in hook.list_keys(bucket, PREFIX) or []
            if (m := RAW_KEY.match(key))
        }
        if not competencias:
            raise RuntimeError(
                "Nenhuma competência encontrada em snh/raw/abecip/. A extração do "
                "relatório mensal não depositou nada — não há o que consolidar."
            )

        registros, digest = [], hashlib.md5()
        for competencia, key in sorted(competencias.items()):
            corpo = hook.read_key(key, bucket).encode("utf-8")
            digest.update(corpo)
            conteudo = json.loads(corpo)
            linhas = (
                conteudo
                if isinstance(conteudo, list)
                else next((v for v in conteudo.values() if isinstance(v, list)), [])
            )
            if not linhas:
                raise RuntimeError(f"Competência {competencia} veio sem registros")
            for linha in linhas:
                # a competência do diretório manda: o campo interno já veio
                # divergente do caminho em extrações anteriores
                registros.append({**linha, "competencia_referencia": competencia})

        with tempfile.TemporaryDirectory(dir=os.environ.get("LAKE_TMPDIR")) as work:
            dst = os.path.join(work, f"{DATASET}.parquet")
            write_text_parquet(
                pd.DataFrame(registros), dst, f"{PREFIX}*/{DATASET}.json", digest.hexdigest(),
                load_id(get_current_context()["dag_run"].run_after),
            )  # fmt: skip
            hook.load_file(dst, STAGING_KEY, bucket, replace=True)

        return {
            "competencias": len(competencias),
            "de": min(competencias),
            "ate": max(competencias),
            "linhas": len(registros),
        }

    @task
    def conferir(resumo: dict) -> None:
        """Guarda contra regressão silenciosa da extração.

        Uma competência que some do raw, ou um arquivo que chega vazio, faria
        o parquet encolher sem erro nenhum — e o gold passaria a mostrar uma
        série mais curta como se fosse o mundo.
        """
        if resumo["competencias"] < 2:
            raise RuntimeError(
                f"Só {resumo['competencias']} competência(s) no raw: a série da "
                "abertura por instituição exige histórico, não um mês solto."
            )
        if resumo["linhas"] < resumo["competencias"] * 10:
            raise RuntimeError(
                f"{resumo['linhas']} linhas para {resumo['competencias']} "
                "competências é pouco: o relatório traz dezenas de linhas por mês."
            )
        print(
            f"{resumo['competencias']} competências consolidadas "
            f"({resumo['de']} a {resumo['ate']}), {resumo['linhas']} linhas."
        )

    conferir(consolidar())


dag_instance = abecip_instituicoes_ingest_dag()
