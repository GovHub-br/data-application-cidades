{# Mesmo padrão do projeto mcid: on-run-start chama isto e os UDFs ficam disponíveis. #}
{% macro create_udfs() %}
    {% if execute %}
        {% do run_query("create schema if not exists " ~ target.schema) %}
        {% do run_query(create_f_parse_date_br()) %}
        {% do log("[poc_silver] UDFs Postgres criados em " ~ target.schema, info=true) %}
    {% endif %}
{% endmacro %}
