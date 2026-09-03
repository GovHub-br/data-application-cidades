-- parse_date_br(col): parse defensivo de data no formato brasileiro e nos
-- formatos que aparecem nas bases MCMV cruas, para os modelos que leem a
-- staging MinIO via DuckDB.
--
-- Porta a UDF Postgres `{{ target.schema }}.parse_date_br`
-- (macros/udfs/f_parse_date_br.sql) para SQL puro DuckDB-safe, trocando o
-- operador regex `~` e `to_char`/`to_date` por `try_strptime` / `try_cast`.
--
-- Formatos cobertos:
--   DD/MM/YYYY            (GFAR, CAIXA)
--   DD/MM/YYYY HH:MM:SS   (timestamps de movimento)
--   YYYYMMDD             (GFAR)
--   YYYY-MM-DD[...]       (ISO — via try_cast)
-- Qualquer coisa que não case → NULL (try_* nunca levanta erro).
{% macro parse_date_br(col) -%}
    {%- set t = "nullif(nullif(trim(cast(" ~ col ~ " as varchar)), ''), 'None')" -%}
    coalesce(
        try_cast(try_strptime({{ t }}, '%d/%m/%Y') as date),
        try_cast(try_strptime({{ t }}, '%d/%m/%Y %H:%M:%S') as date),
        try_cast(try_strptime({{ t }}, '%Y%m%d') as date),
        try_cast({{ t }} as date)
    )
{%- endmacro %}
