"""Consolida as competências da ABECIP por instituição financeira em staging.

O relatório mensal da ABECIP traz a abertura do financiamento imobiliário por
instituição — a tabela que o boletim publica na seção de crédito. Ela é
extraída por outro time, que grava uma competência por diretório em
`raw/abecip/<AAAA-MM>/financiamentos_por_instituicao.json`.

Do nosso lado faltava juntar essas competências numa série. Sem isso o gold
enxergava apenas a última extração, e a abertura por banco ficava restrita a
um mês solto — foi por isso que o quadro da página 3 do boletim conviveu tanto
tempo com a planilha manual, que morria em setembro de 2025.

A DAG lê TODAS as competências presentes no raw e reescreve o parquet inteiro.
Reescrever é de propósito: a ABECIP revisa competências já publicadas, e uma
carga incremental preservaria o número velho ao lado do corrigido.

Não busca nada na internet: consome o que o pipeline de extração já depositou.
"""

from __future__ import annotations

import io
import json
import re
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from cliente_minio import get_s3_client, upload_staging_parquet
from schedule_loader import get_dynamic_schedule

BUCKET = "data-lake-mcid"
PREFIXO = "raw/abecip/"
DADO = "financiamentos_por_instituicao"

def _competencias(cliente) -> dict[str, str]:
    """Competência -> chave do objeto, para tudo que existe no raw."""
    encontradas: dict[str, str] = {}
    token = None
    while True:
        argumentos = {"Bucket": BUCKET, "Prefix": PREFIXO, "MaxKeys": 1000}
        if token:
            argumentos["ContinuationToken"] = token
        resposta = cliente.list_objects_v2(**argumentos)
        for objeto in resposta.get("Contents", []):
            achado = re.match(
                rf"{PREFIXO}(\d{{4}}-\d{{2}})/{DADO}\.json$", objeto["Key"]
            )
            if achado:
                encontradas[achado.group(1)] = objeto["Key"]
        if not resposta.get("IsTruncated"):
            return dict(sorted(encontradas.items()))
        token = resposta.get("NextContinuationToken")


@dag(
    dag_id="abecip_instituicoes_ingest_dag",
    schedule_interval=get_dynamic_schedule(
        "abecip_instituicoes_ingest_dag", default="0 7 * * *"
    ),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["conjuntura", "abecip", "ingestao"],
    description=(
        "Consolida raw/abecip/<competência>/financiamentos_por_instituicao.json "
        "em staging/abecip/financiamentos_por_instituicao.parquet"
    ),
)
def abecip_instituicoes_ingest_dag() -> None:
    @task
    def consolidar() -> dict:
        cliente = get_s3_client()
        competencias = _competencias(cliente)
        if not competencias:
            raise RuntimeError(
                "Nenhuma competência encontrada em raw/abecip/. A extração do "
                "relatório mensal não depositou nada — não há o que consolidar."
            )

        registros: list[dict] = []
        for competencia, chave in competencias.items():
            corpo = cliente.get_object(Bucket=BUCKET, Key=chave)["Body"].read()
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
                linha["competencia_referencia"] = competencia
                registros.append(linha)

        # Sem tipagem aqui de propósito: a staging espelha a forma da origem e
        # quem tipa é a Silver. Converter na ingestão quebrou o cast que a
        # Silver já fazia (`nullif(coluna, '')::numeric` sobre coluna que
        # deixara de ser texto) e duplicaria a regra em duas camadas.
        quadro = pd.DataFrame(registros)

        buffer = io.BytesIO()
        quadro.to_parquet(buffer, engine="pyarrow", index=False)
        upload_staging_parquet("abecip", DADO, buffer.getvalue())

        return {
            "competencias": len(competencias),
            "de": min(competencias),
            "ate": max(competencias),
            "linhas": len(quadro),
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


abecip_instituicoes_ingest_dag()
