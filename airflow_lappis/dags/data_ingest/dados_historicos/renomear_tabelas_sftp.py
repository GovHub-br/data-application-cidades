"""
DAG auxiliar para renomear tabelas já ingeridas no schema sftp.

Lê o mapeamento de renomeação da Airflow Variable 'sftp_rename_map'
e executa ALTER TABLE para cada tabela listada.

Uso:
    1. Crie a Variable 'sftp_rename_map' no Airflow com um JSON:
       {
           "nome_antigo_1": "nome_novo_1",
           "nome_antigo_2": "nome_novo_2"
       }
    2. Execute esta DAG manualmente.
    3. A DAG renomeia as tabelas e atualiza o _ingest_log.
    4. Após a execução, a Variable pode ser limpa.

Feature #90 — Estruturar ingestão e monitoramento dos dados do SFTP.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from contextlib import closing

import json
import logging
import psycopg2

from postgres_helpers import get_postgres_conn

SCHEMA = Variable.get("SFTP_SCHEMA", default_var="sftp_v2") # Colocar sftp depois dos testes
LOG_TABLE = "_ingest_log"

default_args = {
    "owner": "dados_historicos",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def renomear_tabelas(**context: Dict[str, Any]) -> Dict[str, str]:
    """Renomeia tabelas no schema sftp conforme mapeamento da Variable.

    Returns:
        Dict com resultados: {"nome_antigo": "sucesso|erro: msg"}
    """
    try:
        rename_map_raw = Variable.get("sftp_rename_map")
        rename_map = json.loads(rename_map_raw) if isinstance(rename_map_raw, str) else rename_map_raw
    except Exception as e:
        logging.error(
            "Variable 'sftp_rename_map' não encontrada ou inválida: %s", e
        )
        raise ValueError(
            "Crie a Variable 'sftp_rename_map' com um JSON "
            '{"nome_antigo": "nome_novo", ...}'
        ) from e

    if not rename_map:
        logging.info("Nenhuma renomeação configurada.")
        return {}

    conn_str = get_postgres_conn()
    resultados = {}

    with closing(psycopg2.connect(conn_str)) as conn:
        with conn.cursor() as cur:
            for nome_antigo, nome_novo in rename_map.items():
                try:
                    logging.info(
                        "Renomeando %s.%s → %s.%s", SCHEMA, nome_antigo, SCHEMA, nome_novo
                    )

                    # Verifica se a tabela antiga existe
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                        )
                        """,
                        (SCHEMA, nome_antigo),
                    )
                    existe = cur.fetchone()[0]

                    if not existe:
                        msg = f"Tabela {SCHEMA}.{nome_antigo} não existe"
                        logging.warning(msg)
                        resultados[nome_antigo] = f"erro: {msg}"
                        continue

                    # Renomeia
                    cur.execute(
                        f"ALTER TABLE {SCHEMA}.{nome_antigo} "
                        f"RENAME TO {nome_novo};"
                    )

                    # Atualiza o _ingest_log
                    cur.execute(
                        f"""
                        UPDATE {SCHEMA}.{LOG_TABLE}
                        SET nome_tabela = %s
                        WHERE nome_tabela = %s
                        """,
                        (nome_novo, nome_antigo),
                    )

                    conn.commit()
                    resultados[nome_antigo] = "sucesso"
                    logging.info(
                        "  ✔ %s.%s → %s.%s", SCHEMA, nome_antigo, SCHEMA, nome_novo
                    )

                except Exception as e:
                    conn.rollback()
                    msg = str(e).replace('\n', ' ')
                    logging.error(
                        "  ✘ Erro ao renomear %s.%s: %s", SCHEMA, nome_antigo, msg
                    )
                    resultados[nome_antigo] = f"erro: {msg}"

    # Resumo
    logging.info("=" * 60)
    logging.info("RESUMO DA RENOMEAÇÃO")
    logging.info("=" * 60)
    for antigo, resultado in resultados.items():
        novo = rename_map.get(antigo, "?")
        logging.info("  %s → %s: %s", antigo, novo, resultado)

    return resultados


with DAG(
    dag_id="sftp_renomear_tabelas",
    default_args=default_args,
    description="Renomeia tabelas do schema sftp conforme Variable sftp_rename_map",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sftp", "dados_historicos", "util"],
) as dag:

    PythonOperator(
        task_id="renomear_tabelas",
        python_callable=renomear_tabelas,
    )
