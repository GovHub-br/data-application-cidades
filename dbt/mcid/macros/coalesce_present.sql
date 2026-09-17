-- present_columns(relation, aliases)
--
-- Filtra `aliases` mantendo so os que existem de fato na `relation` (consultada
-- no banco em tempo de compilacao). Base de coalesce_present() e de
-- coalesce_present_parsed().
--
-- - Durante `dbt run`, a relacao ja esta materializada (o dbt constroi as deps
-- antes): o filtro usa as colunas reais.
-- - Durante `dbt parse`/`compile` sem a relacao materializada: devolve lista
-- vazia. Compila; o valor real so importa no run.
{% macro present_columns(relation, aliases) %}
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
    {{ return(present) }}
{% endmacro %}

-- coalesce_present(relation, aliases, cast_type)
--
-- Monta um coalesce() usando SOMENTE as colunas de `aliases` que existem de fato
-- na `relation`. Serve para modelos que unem varias geracoes de schema de uma
-- mesma familia de arquivos, onde uma coluna pode nao existir em nenhum arquivo
-- daquele lote.
{% macro coalesce_present(relation, aliases, cast_type=none) %}
    {%- set present = present_columns(relation, aliases) -%}
    {%- if present | length > 0 -%}
        {%- set expr = 'coalesce(' ~ (present | join(', ')) ~ ')' -%}
    {%- else -%} {%- set expr = 'null' -%}
    {%- endif -%}
    {%- if cast_type is not none -%} cast({{ expr }} as {{ cast_type }})
    {%- else -%} {{ expr }}
    {%- endif -%}
{% endmacro %}

-- coalesce_present_parsed(relation, aliases, parser, cast_null_as)
--
-- Como coalesce_present(), mas aplica o parser a CADA alias presente antes do
-- coalesce — `coalesce(parse(a), parse(b))`, nao `parse(coalesce(a, b))`. A
-- diferenca importa: um valor nao-nulo mas impossivel de converter em `a` deve
-- deixar `b` assumir, e nao virar NULL.
--
-- `cast_null_as` tipa o NULL quando nenhum alias existe na relacao — necessario
-- para que bracos de um `union all` tenham o mesmo tipo.
{% macro coalesce_present_parsed(relation, aliases, parser, cast_null_as=none) %}
    {%- set present = present_columns(relation, aliases) -%}
    {%- set parts = [] -%}
    {%- for a in present -%}
        {%- if parser == 'parse_hist_bigint' -%}
            {%- do parts.append(parse_hist_bigint(a)) -%}
        {%- elif parser == 'parse_hist_double' -%}
            {%- do parts.append(parse_hist_double(a)) -%}
        {%- elif parser == 'parse_hist_date' -%}
            {%- do parts.append(parse_hist_date(a)) -%}
        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "coalesce_present_parsed: parser '" ~ parser ~ "' nao suportado"
            ) }}
        {%- endif -%}
    {%- endfor -%}
    {%- if parts | length == 0 -%}
        {%- if cast_null_as is not none -%}cast(null as {{ cast_null_as }})
        {%- else -%}null
        {%- endif -%}
    {%- elif parts | length == 1 -%}{{ parts[0] }}
    {%- else -%}coalesce({{ parts | join(', ') }})
    {%- endif -%}
{% endmacro %}
