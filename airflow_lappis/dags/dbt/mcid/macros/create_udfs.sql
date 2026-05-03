{% macro create_udfs() %}

create schema if not exists {{ target.schema }};

    {{ create_f_parse_date_br() }}
    ;
    {{ create_f_normalize_apf() }}
    ;

{% endmacro %}
