{#
    Verdadeiro quando `expressao` contém um trecho que casa com `padrao`, nos dois targets.

    O nome igual engana: no Postgres o `regexp_matches()` devolve um conjunto de linhas e não
    pode ficar no `where` (é o `~` que testa); no DuckDB o `~` exige o texto inteiro, e quem
    procura um trecho é o `regexp_matches()`.
#}
{% macro casa_regex(expressao, padrao) -%}
    {%- if target.type == 'postgres' -%} ({{ expressao }}) ~ '{{ padrao }}'
    {%- else -%} regexp_matches({{ expressao }}, '{{ padrao }}')
    {%- endif -%}
{%- endmacro %}
