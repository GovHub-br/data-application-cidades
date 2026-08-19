-- UDF para parsear datas no formato brasileiro DD/MM/YYYY
{% macro create_f_parse_date_br() %}

    create or replace function {{ target.schema }}.parse_date_br(in_text text)
    returns date
    as
        $$
        select
            case
                when in_text is null or trim(in_text) = '' then null
                -- Formato DD/MM/YYYY
                when in_text ~ '^\d{2}/\d{2}/\d{4}$'
                then to_date(in_text, 'DD/MM/YYYY')
                -- Formato YYYYMMDD (usado no GFAR)
                when in_text ~ '^\d{8}$'
                then to_date(in_text, 'YYYYMMDD')
                -- Formato YYYY-MM-DD
                when in_text ~ '^\d{4}-\d{2}-\d{2}'
                then in_text::date
                else null
            end
    $$
    language sql
    ;

{% endmacro %}
