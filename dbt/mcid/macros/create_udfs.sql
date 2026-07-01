{% macro create_udfs() %}
-- Lock para evitar problemas de concorrência
-- na chamada da macro no Cosmos
select pg_advisory_xact_lock(123456789);
create schema if not exists {{ target.schema }};

    {{ create_f_parse_date_br() }}
    ;
    {{ create_f_normalize_apf() }}
    ;

{% endmacro %}
