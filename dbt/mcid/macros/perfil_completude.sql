{#
    Gera uma linha por coluna de cada model informado, com a contagem de
    nulos e o % de completude.

    Existe porque "a base está completa?" não é respondível olhando model por
    model — e porque completude que ninguém mede vira surpresa no dashboard.
    O resultado é materializado em `gold_qualidade_completude`, então dá pra
    acompanhar a evolução e pendurar teste em cima.

    Usa o catálogo do Postgres (`information_schema`) pra descobrir as
    colunas em tempo de compilação, e monta um UNION ALL de contagens.
#}
{% macro perfil_completude(schemas) %}

    {%- if execute -%}
        {%- set consulta -%}
            select table_schema, table_name, column_name
            from information_schema.columns
            where table_schema in (
                {%- for s in schemas -%}'{{ s }}'{{ ", " if not loop.last }}{%- endfor -%}
            )
              and column_name not in ('dt_ingest', '_source_file', '_ingested_at', '_source_hash')
            order by table_schema, table_name, ordinal_position
        {%- endset -%}
        {%- set linhas = run_query(consulta).rows -%}

        {%- for linha in linhas %}
        select
            '{{ linha[0] }}'::text as schema_dado,
            '{{ linha[1] }}'::text as model,
            '{{ linha[2] }}'::text as coluna,
            count(*)::bigint       as linhas,
            count("{{ linha[2] }}")::bigint as preenchidas,
            case when count(*) = 0 then null
                 else round(count("{{ linha[2] }}")::numeric / count(*), 4)
            end as completude,
            current_timestamp      as medido_em
        from {{ linha[0] }}."{{ linha[1] }}"
        {{ "union all" if not loop.last }}
        {%- endfor %}
    {%- else -%}
        select null::text as schema_dado, null::text as model, null::text as coluna,
               null::bigint as linhas, null::bigint as preenchidas,
               null::numeric as completude, current_timestamp as medido_em
        where false
    {%- endif -%}

{% endmacro %}
