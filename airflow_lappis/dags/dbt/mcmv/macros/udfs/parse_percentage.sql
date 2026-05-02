{% macro parse_percentage(column_name) %}
case
    when {{ column_name }} is null or trim({{ column_name }}) = ''
    then 0.00::numeric(10,2)
    else replace({{ column_name }}, ',', '.')::numeric(10,2)
end
{% endmacro %}