-- Trata as aspas vazias '' e também a string literal 'None',
-- substituindo ambas por NULL antes de fazer o cast direto para int.

{% macro parse_int(column_name) %}
    nullif(nullif(trim({{ column_name }}), ''), 'None')::int
{% endmacro %}

-- Trata espaços em branco e o string literal 'None',
-- além de já realizar o replace de , por . antes de fazer o cast.

{% macro parse_numeric(column_name, cast_type='numeric') %}
    nullif(replace(nullif(trim({{ column_name }}), 'None'), ',', '.'), '')::{{ cast_type }}
{% endmacro %}
