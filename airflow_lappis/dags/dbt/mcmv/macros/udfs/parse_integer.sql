{% macro parse_integer_value(column_name) %}

case
    when {{ column_name }} is null 
        or trim({{ column_name }}) = ''
        or trim({{ column_name }}) ilike 'none'
    then 0
    else replace({{ column_name }}, ',', '.')::numeric::integer
end

{% endmacro %}