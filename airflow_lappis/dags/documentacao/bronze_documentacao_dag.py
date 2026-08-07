import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import psycopg2
from airflow.decorators import dag, task
from airflow.models import Variable
from documentador_bronze import (
    LIMITE_AMOSTRA,
    NOME_ARQUIVO,
    SCHEMAS_PADRAO,
    carregar_anterior,
    carregar_descricoes_dbt,
    diretorio_de_trabalho,
    escrever_yaml,
    montar_documento,
    perfilar_schema,
    tabela_de_json,
    tabela_para_json,
)
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

# O bronze.yml é gravado na raiz dos projetos dbt, junto do `.user.yml`.
# `dags/` vem montado do repositório, então escrever aqui grava direto em
# `airflow_lappis/dags/dbt/bronze.yml` na árvore versionada — que é o objetivo.
#
# Este nível fica ACIMA de ipea/mcid/mir: nenhum `model-paths` o alcança, então
# o dbt não lê o arquivo como um `sources` duplicado dos que já estão declarados.
RAIZ_DBT = Variable.get(
    "DOCUMENTACAO_BRONZE_RAIZ_DBT", default_var="/opt/airflow/dags/dbt"
)
DIRETORIO_SAIDA = Variable.get("DOCUMENTACAO_BRONZE_DIR", default_var=RAIZ_DBT)

SCHEMAS = Variable.get(
    "DOCUMENTACAO_BRONZE_SCHEMAS", deserialize_json=True, default_var=list(SCHEMAS_PADRAO)
)


@dag(
    schedule_interval=get_dynamic_schedule("bronze_documentacao_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Bottino",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["documentacao", "metadados", "bronze", "openmetadata"],
)
def bronze_documentacao_dag() -> None:
    """Documenta as bases brutas do DW num único `bronze.yml`.

    Para cada schema: lista as tabelas, agrupa as que são snapshots irmãos
    (`..._m1` .. `..._m182`) numa família só, perfila uma representante lendo os
    primeiros 10k registros e coleta estatística por coluna.

    Só grava fato verificável. Descrição em linguagem natural não é gerada aqui:
    quando já existe texto escrito à mão nos YAMLs do dbt, ele é reaproveitado.

    Valores de exemplo saem apenas de colunas sem indício de dado pessoal —
    coluna sensível fica só com estatística agregada.
    """

    @task
    def perfilar(schema: str) -> str:
        """Perfila um schema e grava o parcial em disco.

        O resultado vai para arquivo, e não para XCom, porque o perfil completo
        de um schema grande passa de 1 MB — tamanho que não se deve empurrar
        pela tabela de metadados do Airflow.
        """
        anterior = carregar_anterior(Path(DIRETORIO_SAIDA) / NOME_ARQUIVO)

        conn = psycopg2.connect(get_postgres_conn())
        try:
            tabelas = perfilar_schema(conn, schema, LIMITE_AMOSTRA, anterior)
        finally:
            conn.close()

        # Parcial é andaime: vai para temp, não para a árvore versionada.
        parcial = diretorio_de_trabalho() / f"{schema}.json"
        parcial.parent.mkdir(parents=True, exist_ok=True)
        parcial.write_text(
            json.dumps([tabela_para_json(t) for t in tabelas], ensure_ascii=False),
            encoding="utf-8",
        )
        logging.info(
            "[bronze_documentacao] %s: %d tabelas -> %s", schema, len(tabelas), parcial
        )
        return str(parcial)

    @task
    def consolidar(parciais: List[str]) -> str:
        """Junta os parciais num único bronze.yml no padrão `sources` do dbt."""
        por_schema: Dict[str, List[Any]] = {}
        for caminho in parciais:
            arquivo = Path(caminho)
            if not arquivo.exists():
                logging.warning("[bronze_documentacao] parcial ausente: %s", caminho)
                continue
            tabelas = [
                tabela_de_json(t) for t in json.loads(arquivo.read_text(encoding="utf-8"))
            ]
            if tabelas:
                por_schema[tabelas[0].schema] = tabelas

        descricoes = carregar_descricoes_dbt(Path(RAIZ_DBT))
        destino = escrever_yaml(
            montar_documento(por_schema, descricoes), Path(DIRETORIO_SAIDA) / NOME_ARQUIVO
        )
        total = sum(len(v) for v in por_schema.values())
        logging.info(
            "[bronze_documentacao] %d tabelas de %d schemas em %s",
            total,
            len(por_schema),
            destino,
        )
        return str(destino)

    consolidar(perfilar.expand(schema=SCHEMAS))


bronze_documentacao_dag()
