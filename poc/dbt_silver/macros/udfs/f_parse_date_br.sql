-- Cópia FIEL do UDF de produção (airflow_lappis/dags/dbt/mcid/macros/udfs/f_parse_date_br.sql).
-- Está aqui sem uma vírgula de alteração de propósito: é a prova de que a arquitetura nova
-- não obriga a portar os UDFs Postgres para DuckDB. Eles continuam rodando no Postgres,
-- sobre a view da bronze.
{% macro create_f_parse_date_br() %}

    create or replace function {{ target.schema }}.parse_date_br(in_text text)
    returns date
    as
        $$
        select
            case
                when in_text is null or trim(in_text) = '' then null
                when in_text ~ '^\d{2}/\d{2}/\d{4}$'
                then to_date(in_text, 'DD/MM/YYYY')
                when in_text ~ '^\d{8}$'
                then to_date(in_text, 'YYYYMMDD')
                when in_text ~ '^\d{4}-\d{2}-\d{2}'
                then in_text::date
                else null
            end
    $$
    language sql
    ;

{% endmacro %}
