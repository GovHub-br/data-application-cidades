-- coalesce_present(relation, aliases, cast_type)
--
-- Monta um coalesce() usando SOMENTE as colunas de `aliases` que existem de fato
-- na `relation` (consultada no banco em tempo de compilacao). Serve para modelos
-- que unem varias geracoes de schema de uma mesma familia de arquivos, onde uma
-- coluna pode nao existir em nenhum arquivo daquele lote.
--
-- - Durante `dbt run`, a relacao ja esta materializada (o dbt constroi as deps
-- antes): o filtro usa as colunas reais.
-- - Durante `dbt parse`/`compile` sem a relacao materializada: retorna `null`
-- (ou `cast(null as <tipo>)`). Compila; o valor real so importa no run.
{% macro coalesce_present(relation, aliases, cast_type=none) %}
    {%- set present = [] -%}
    {%- if execute -%}
        {%- set rel = adapter.get_relation(
            database=relation.database,
            schema=relation.schema,
            identifier=relation.identifier
        ) -%}
        {%- if rel is not none -%}
            {%- set cols = [] -%}
            {%- for c in adapter.get_columns_in_relation(rel) -%}
                {%- do cols.append(c.name | lower) -%}
            {%- endfor -%}
            {%- for a in aliases -%}
                {%- if (a | lower) in cols -%} {%- do present.append(a) -%} {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}
    {%- if present | length > 0 -%}
        {%- set expr = 'coalesce(' ~ (present | join(', ')) ~ ')' -%}
    {%- else -%} {%- set expr = 'null' -%}
    {%- endif -%}
    {%- if cast_type is not none -%} cast({{ expr }} as {{ cast_type }})
    {%- else -%} {{ expr }}
    {%- endif -%}
{% endmacro %}
