# Plugin dbt-duckdb: porta pro DuckDB (targets staging_duckdb/prod_duckdb) as
# 3 UDFs que em Postgres (target prod) sao criadas por create_udfs()
# (macros/create_udfs.sql), hook condicionado a `target.type == 'postgres'`
# (dbt_project.yml, on-run-start) - por isso nunca existiam em DuckDB e todo
# modelo far_dbt/fds_dbt/rural_dbt que chama metadata.normalize_apf/
# parse_date_br/corrigir_mojibake quebrava com "Catalog Error" nesse target.
#
# normalize_apf e parse_date_br sao SQL puro (lpad/regexp_replace/strptime) e
# viram CREATE MACRO direto - fieis ao original, sem reescrever a logica.
# corrigir_mojibake precisa de round-trip de encoding (WIN1252/LATIN1 -> UTF8)
# com loop e exception handling: DuckDB SQL nao tem convert_from/convert_to
# nem try/catch dentro de macro, entao essa via Python (create_function).
#
# GOTCHA: DuckDBPyConnection.create_function() SEMPRE registra no schema
# `main`, mesmo passando um nome qualificado tipo "metadata.corrigir_mojibake"
# (confirmado empiricamente - a API nao documenta isso e nao expoe parametro
# de schema). Por isso a funcao Python fica em `main.corrigir_mojibake` e uma
# CREATE MACRO em metadata.corrigir_mojibake so delega pra ela - e a MACRO
# (objeto de catalogo) que fica visivel pra qualquer cursor derivado da mesma
# conexao (testado: nao precisa re-registrar a funcao Python por cursor).
#
# Wiring: profiles.yml (targets staging_duckdb/prod_duckdb) declara
# `plugins: [{module: udfs_duckdb}]` + `module_paths: [plugins]` (relativo a
# dbt/mcid/, de onde todo run-*.sh/_run-common.sh sempre invoca o dbt).
import re

from dbt.adapters.duckdb.plugins import BasePlugin
from duckdb import DuckDBPyConnection

_MOJIBAKE_MARKER = re.compile(r"[ÃÂ]|â€")


def _corrigir_mojibake(texto):
    if texto is None:
        return None
    atual = texto
    # Ate 3 passadas: ha texto que passou pelo round-trip mais de uma vez
    # ("SÃƒÂ£o" e o mojibake do mojibake de "Sao"). Para no ponto fixo - mesmo
    # limite do original plpgsql (macros/udfs/f_corrigir_mojibake.sql).
    for _ in range(3):
        if not _MOJIBAKE_MARKER.search(atual):
            return atual
        try:
            # WIN1252 primeiro, nao LATIN1: a corrupcao veio de ler utf-8
            # como cp1252, e so o cp1252 tem €/aspas curvas/travessao no
            # intervalo 0x80-0x9F que aparece nos dados de origem (mesmo
            # motivo documentado no original).
            tentativa = atual.encode("cp1252").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            try:
                tentativa = atual.encode("latin1").decode("utf-8")
            except (UnicodeDecodeError, UnicodeEncodeError):
                return atual
        if tentativa == atual:
            return atual
        atual = tentativa
    return atual


_NORMALIZE_APF_MACRO = """
    create or replace macro {schema}.normalize_apf(in_text) as (
        case
            when in_text is null or trim(in_text) = '' then null
            when in_text like '%-%'
            then lpad(replace(in_text, '-', ''), 8, '0')
            when length(regexp_replace(in_text, '[^0-9]', '', 'g')) >= 8
            then right(regexp_replace(in_text, '[^0-9]', '', 'g'), 8)
            else lpad(regexp_replace(in_text, '[^0-9]', '', 'g'), 8, '0')
        end
    )
"""

_PARSE_DATE_BR_MACRO = r"""
    create or replace macro {schema}.parse_date_br(in_text) as (
        case
            when in_text is null or trim(in_text) = '' then null
            when regexp_matches(in_text, '^\d{{2}}/\d{{2}}/\d{{4}}$')
            then strptime(in_text, '%d/%m/%Y')::date
            when regexp_matches(in_text, '^\d{{8}}$')
            then strptime(in_text, '%Y%m%d')::date
            when regexp_matches(in_text, '^\d{{4}}-\d{{2}}-\d{{2}}')
            then in_text::date
            else null
        end
    )
"""


class Plugin(BasePlugin):
    def initialize(self, plugin_config):
        # Mesmo default do var `schema_udfs` em dbt_project.yml (fixo por
        # projeto, nao por profile/maquina - ver comentario la).
        self.schema = plugin_config.get("schema_udfs", "metadata")

    def configure_connection(self, conn: DuckDBPyConnection) -> None:
        conn.execute(f"create schema if not exists {self.schema}")
        conn.execute(_NORMALIZE_APF_MACRO.format(schema=self.schema))
        conn.execute(_PARSE_DATE_BR_MACRO.format(schema=self.schema))
        conn.create_function(
            "corrigir_mojibake", _corrigir_mojibake, ["VARCHAR"], "VARCHAR"
        )
        conn.execute(
            f"create or replace macro {self.schema}.corrigir_mojibake(x) "
            "as (main.corrigir_mojibake(x))"
        )
