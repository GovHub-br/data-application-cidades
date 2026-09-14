{#
    Introspeccao das colunas de um glob da staging, em tempo de compilacao.

    E o analogo de `coalesce_present()` (que consulta uma RELACAO ja
    materializada) para a camada bronze, que le PARQUET direto: as familias
    tem 2-3 geracoes de schema e uma coluna pode nao existir em nenhum
    arquivo daquele lote. Sem isso, referenciar a coluna ausente e erro de
    binder no DuckDB — o que so nao acontecia antes porque o
    `union_by_name` entre familias preenchia as lacunas com null.

    Le apenas o rodape dos parquets (`describe`), nao os dados.

    Durante `dbt parse` (sem `execute`) devolve lista vazia / `null`, como
    `coalesce_present` — compila; o valor real so importa no run.
#}

{% macro staging_glob_columns(glob_pattern) %}
    {%- set cols = [] -%}
    {%- if execute -%}
        {%- set res = run_query(
            'describe select * from ' ~ read_minio_staging_parquet_series(glob_pattern)
        ) -%}
        {%- if res is not none -%}
            {%- for r in res.rows -%}{%- do cols.append(r[0] | lower) -%}{%- endfor -%}
        {%- endif -%}
    {%- endif -%}
    {{ return(cols) }}
{% endmacro %}

{#
    coalesce() sobre as colunas de `aliases` presentes em `cols`
    (`staging_glob_columns` de uma familia).

    - `try_cast_as`: envolve cada alias presente em `try_cast(<a> as <tipo>)`.
    - `cast_type`: envolve o coalesce inteiro em `cast(... as <tipo>)`.
#}
{% macro coalesce_present_cols(cols, aliases, cast_type=none, try_cast_as=none) %}
    {%- set present = [] -%}
    {%- for a in aliases -%}
        {%- if (a | lower) in cols -%}
            {%- if try_cast_as is not none -%}
                {%- do present.append('try_cast(' ~ a ~ ' as ' ~ try_cast_as ~ ')') -%}
            {%- else -%}
                {%- do present.append(a) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if present | length > 0 -%}
        {%- set expr = 'coalesce(' ~ (present | join(', ')) ~ ')' -%}
    {%- else -%}
        {%- set expr = 'null' -%}
    {%- endif -%}
    {%- if cast_type is not none -%}cast({{ expr }} as {{ cast_type }})
    {%- else -%}{{ expr }}
    {%- endif -%}
{% endmacro %}
