{% macro parse_financial_value(column_name) %}

    case
        when {{ column_name }} is null or trim({{ column_name }}) = '' or trim({{ column_name }}) = 'None'
        then 0.00::numeric(15, 2)

        when {{ column_name }} like '%NaN%'
        then 0.00::numeric(15, 2)
        -- Formato GFAR: "0000000034679700,00" (zeros à esquerda, vírgula decimal)
        when {{ column_name }} ~ '^0+\d+,\d+$'
        then replace(
            ltrim({{ column_name }}, '0'),
            ',', '.'
        )::numeric(15, 2)
        -- Formato brasileiro padrão: "34.679.700,00" (ponto milhar, vírgula decimal)
        when {{ column_name }} like '%,%' and {{ column_name }} like '%.%'
        then replace(
            replace(coalesce({{ column_name }}, '0'), '.', ''),
            ',', '.'
        )::numeric(15, 2)
        -- Formato com apenas vírgula decimal: "34679700,00"
        when {{ column_name }} like '%,%'
        then replace(
            coalesce({{ column_name }}, '0'),
            ',', '.'
        )::numeric(15, 2)
        -- Formato já numérico: "34679700.00"
        else coalesce(nullif(trim({{ column_name }}), ''), '0')::numeric(15, 2)
    end

{% endmacro %}
