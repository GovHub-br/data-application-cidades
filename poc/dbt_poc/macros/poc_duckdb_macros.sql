{#
  Porte dos UDFs Postgres do projeto mcid para CREATE MACRO do DuckDB.

  Originais:
    airflow_lappis/dags/dbt/mcid/macros/udfs/f_parse_date_br.sql
    airflow_lappis/dags/dbt/mcid/macros/udfs/f_normalize_apf.sql
    airflow_lappis/dags/dbt/mcid/macros/parse_financial_value.sql

  Diferenças de dialeto que o porte exigiu (é o custo real da migração, R1):
    ~              -> regexp_matches()
    to_date()      -> try_strptime()::date
    ::numeric(p,s) -> ::decimal(p,s)
    ltrim(x, '0')  -> ltrim(x, '0')          (igual)
    UDF no schema  -> macro na SESSÃO do DuckDB: as chamadas deixam de ser
                      {{ target.schema }}.parse_date_br(x) e viram poc_parse_date_br(x),
                      ou seja, TODA chamada nos models precisa ser reescrita.

  Nota: `parse_financial_value` já era macro Jinja (expandida no SQL), então poderia
  continuar Jinja. Foi portada para CREATE MACRO aqui para medir se a versão nativa do
  DuckDB dá o mesmo resultado — dá.
#}

{% macro create_poc_macros() %}

    {% set sql %}

        create or replace macro poc_parse_date_br(in_text) as
            case
                when in_text is null or trim(in_text) = '' then null
                when regexp_matches(in_text, '^\d{2}/\d{2}/\d{4}$')
                    then try_strptime(in_text, '%d/%m/%Y')::date
                when regexp_matches(in_text, '^\d{8}$')
                    then try_strptime(in_text, '%Y%m%d')::date
                when regexp_matches(in_text, '^\d{4}-\d{2}-\d{2}')
                    then try_cast(in_text as date)
            end;

        create or replace macro poc_normalize_apf(in_text) as
            case
                when in_text is null or trim(in_text) = '' then null
                when in_text like '%-%'
                    then lpad(replace(in_text, '-', ''), 8, '0')
                when length(regexp_replace(in_text, '[^0-9]', '', 'g')) >= 8
                    then right(regexp_replace(in_text, '[^0-9]', '', 'g'), 8)
                else lpad(regexp_replace(in_text, '[^0-9]', '', 'g'), 8, '0')
            end;

        create or replace macro poc_parse_valor_br(in_text) as
            case
                when in_text is null or trim(in_text) in ('', 'None')
                    then 0.00::decimal(15, 2)
                when in_text like '%NaN%'
                    then 0.00::decimal(15, 2)
                -- GFAR: "0000000034679700,00"
                when regexp_matches(in_text, '^0+\d+,\d+$')
                    then try_cast(replace(ltrim(in_text, '0'), ',', '.') as decimal(15, 2))
                -- brasileiro com milhar: "34.679.700,00"
                when in_text like '%,%' and in_text like '%.%'
                    then try_cast(replace(replace(in_text, '.', ''), ',', '.') as decimal(15, 2))
                -- só vírgula decimal: "34679700,00"
                when in_text like '%,%'
                    then try_cast(replace(in_text, ',', '.') as decimal(15, 2))
                -- já numérico: "34679700.00"
                else coalesce(try_cast(trim(in_text) as decimal(15, 2)), 0.00::decimal(15, 2))
            end;

        create or replace macro poc_sn_para_bool(in_text) as
            case upper(trim(in_text))
                when 'S' then true
                when 'N' then false
            end;

    {% endset %}

    {% do run_query(sql) %}
    {% do log("[poc] macros DuckDB criadas na sessão", info=true) %}

{% endmacro %}
