{% macro parse_financial_value(column_name) %}

case
    when {{ column_name }} is null
        or trim({{ column_name }}) = ''
        or {{ column_name }} ilike '%nan%'
        or trim({{ column_name }}) ilike 'none'
    then 0.00::numeric(15,2)

    when {{ column_name }} like '%(%'
    then
        regexp_replace(
            replace(
                replace(
                    replace(trim({{ column_name }}), 'R$', ''),
                '.', ''),
            ',', '.'),
            '(\()?(\d+(\.\d+)?)(\))?',
            '-\2'
        )::numeric(15,2)

    else
        replace(
            replace(
                replace(trim({{ column_name }}), 'R$', ''),
            '.', ''),
        ',', '.')::numeric(15,2)
end

{% endmacro %}