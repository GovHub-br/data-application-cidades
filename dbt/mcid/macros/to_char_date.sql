{#
    Formata data como texto — portável entre Postgres (to_char) e DuckDB
    (strftime, sem to_char nativo). Mesmo formato lógico, sintaxe de máscara
    diferente em cada dialeto: passe os dois.

    Uso:
        {{ to_char_date('m.mes', 'YYYY-MM-DD', '%Y-%m-%d') }} as mes,
#}
{% macro to_char_date(col, pg_fmt, duckdb_fmt) -%}
{%- if target.type == 'duckdb' -%}
strftime({{ col }}, '{{ duckdb_fmt }}')
{%- else -%}
to_char({{ col }}, '{{ pg_fmt }}')
{%- endif -%}
{%- endmacro %}
