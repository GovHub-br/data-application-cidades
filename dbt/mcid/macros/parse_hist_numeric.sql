-- Parsers defensivos para as series historicas do dump `dados_historicos`,
-- lidas via DuckDB. O dump tratado ja normaliza a maioria dos numeros para o
-- formato dot-decimal (ex. "2113.0"), mas ainda ha residuos em formato
-- brasileiro ("2.113,00") e strings "None"/"nan". Estas macros absorvem os dois.

{% macro parse_hist_double(col) -%}
    try_cast(
        case
            when {{ col }} is null then null
            when trim(cast({{ col }} as varchar)) in ('', 'None', 'nan', 'NaN', 'null', 'NULL') then null
            when cast({{ col }} as varchar) like '%,%' and cast({{ col }} as varchar) like '%.%'
                then replace(replace(cast({{ col }} as varchar), '.', ''), ',', '.')
            when cast({{ col }} as varchar) like '%,%'
                then replace(cast({{ col }} as varchar), ',', '.')
            else cast({{ col }} as varchar)
        end
    as double)
{%- endmacro %}

{% macro parse_hist_bigint(col) -%}
    try_cast(round({{ parse_hist_double(col) }}) as bigint)
{%- endmacro %}

-- parse_hist_numeric(col): valor monetário como numeric(15,2). Substitui a macro
-- Postgres `parse_financial_value` (que usa o operador `~`) nos modelos que leem
-- a staging MinIO via DuckDB. Reusa `parse_hist_double`, que já absorve o formato
-- GFAR com zeros à esquerda ("0000000034679700,00"), o brasileiro ("34.679.700,00")
-- e o dot-decimal. Diferença vs. `parse_financial_value`: entrada nula/vazia/`NaN`
-- vira NULL (não `0.00`) — comportamento medalhão correto; conferir na
-- reconciliação de somas (task 6.5) que nenhuma agregação regride.
{% macro parse_hist_numeric(col) -%}
    try_cast({{ parse_hist_double(col) }} as numeric(15, 2))
{%- endmacro %}

{% macro parse_hist_date(col) -%}
    try_cast(nullif(nullif(trim(cast({{ col }} as varchar)), ''), 'None') as date)
{%- endmacro %}

-- Mes de referencia (primeiro dia) a partir de report_date (preferencial) OU do
-- nome do arquivo. Cobre: report_date ISO; YYYYMMDD / DDMMYYYY (8 digitos);
-- YYYY[_-]MM; e "<mes-abrev><ano>" em pt-BR (ex. abr2018, dez17, jan_2017).
{% macro hist_dt_referencia(report_date_col, filename_col) -%}
    {%- set fn = "lower(regexp_replace(cast(" ~ filename_col ~ " as varchar), '^.*/', ''))" -%}
    {%- set mabbr = "regexp_extract(" ~ fn ~ ", '(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[_ ]?(\\d{2}|\\d{4})(\\D|$)', 1)" -%}
    {%- set yabbr = "regexp_extract(" ~ fn ~ ", '(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[_ ]?(\\d{2}|\\d{4})(\\D|$)', 2)" -%}
    date_trunc('month', try_cast(
        coalesce(
            try_cast(nullif(nullif(trim(cast({{ report_date_col }} as varchar)), ''), 'None') as date),
            try_strptime(regexp_extract({{ fn }}, '(20[0-2]\d)(0[1-9]|1[0-2])[0-3]\d', 0), '%Y%m%d'),
            try_strptime(regexp_extract({{ fn }}, '[0-3]\d(0[1-9]|1[0-2])(20[0-2]\d)', 0), '%d%m%Y'),
            try_strptime(
                nullif(regexp_extract(
                    regexp_replace({{ fn }}, '(20[0-2]\d)[_-](0[1-9]|1[0-2])', '\1\2'),
                    '20[0-2]\d(?:0[1-9]|1[0-2])', 0), ''),
                '%Y%m'),
            case when {{ mabbr }} <> '' and {{ yabbr }} <> '' then try_strptime(
                (case when length({{ yabbr }}) = 2 then '20' || {{ yabbr }} else {{ yabbr }} end)
                || (case {{ mabbr }}
                        when 'jan' then '01' when 'fev' then '02' when 'mar' then '03'
                        when 'abr' then '04' when 'mai' then '05' when 'jun' then '06'
                        when 'jul' then '07' when 'ago' then '08' when 'set' then '09'
                        when 'out' then '10' when 'nov' then '11' when 'dez' then '12' end)
                || '01', '%Y%m%d') end
        ) as date
    ))::date
{%- endmacro %}

-- Compat: versao so-do-nome-do-arquivo (usada onde nao ha report_date).
{% macro hist_dt_referencia_from_filename(filename_col) -%}
    {{ hist_dt_referencia("cast(null as varchar)", filename_col) }}
{%- endmacro %}
