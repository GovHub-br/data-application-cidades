{#
    Macros do produto Trabalho Social (models/trabalho_social_dbt).

    bnz_ts_staging  -> bronze: cópia fiel da staging, tudo texto.
    ts_texto        -> prata: texto limpo (mojibake, espaços, marcadores de vazio).
    ts_numero       -> prata: número em texto ("83.62", "1767180", "0,95") para numeric.
    ts_data         -> prata: data em texto ISO ou dd/mm/aaaa; datas-sentinela viram nulo.
    ts_dt_referencia_arquivo -> prata: competência (último dia do mês) lida do nome do arquivo.
#}

{% macro bnz_ts_staging(padrao, colunas, todos_os_arquivos=false, excluir=[]) -%}
    {#-
        Bronze do Trabalho Social: espelho da staging do MinIO, com TODAS as colunas em texto.

        - `padrao`: glob do parquet na staging.
        - `colunas`: layout conhecido da planilha. Entra como molde vazio unido POR NOME ao
          parquet, para que uma remessa sem alguma coluna (a planilha de 06/10/2026 dos
          empreendimentos sem indicação de demanda, por exemplo, não traz UF nem a situação do
          TS) não quebre a prata: a coluna existe e vem nula. Coluna nova que a origem mandar
          passa também, porque o `union all by name` preserva o que só existe no parquet.
        - `todos_os_arquivos`: false = só o arquivo mais recente (cada remessa é um retrato
          completo); true = empilha todas as remessas, para a prata escolher por APF.
        - `excluir`: termos que tiram um arquivo da disputa do mais recente.
    -#}
    with
        molde as (
            select
                {%- for c in colunas %}
                cast(null as varchar) as "{{ c }}",
                {%- endfor %}
                cast(null as varchar) as "_source_file",
                cast(null as varchar) as "_ingested_at",
                cast(null as varchar) as "_source_hash",
                cast(null as varchar) as "filename"
            where false
        ),

        staging as (
            select *
            from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
            {%- if not todos_os_arquivos %}
            where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao, excluir) }}
            {%- endif %}
        ),

        unido as (
            select * from molde
            union all by name
            select * from staging
        )

    select columns(*)::varchar
    from unido
{%- endmacro %}


{% macro ts_texto(coluna) -%}
    case
        when {{ coluna }} is null then null
        when upper(trim({{ coluna }}::text)) in ('', '-', 'NONE', 'NAN', 'NULL', 'NAT') then null
        else trim({{ var('schema_udfs') }}.corrigir_mojibake(trim({{ coluna }}::text)))
    end
{%- endmacro %}


{% macro ts_numero(coluna, tipo="numeric(18, 4)") -%}
    case
        when trim({{ coluna }}::text) ~ '^-?\d+(\.\d+)?$' then trim({{ coluna }}::text)::{{ tipo }}
        when trim({{ coluna }}::text) ~ '^-?\d+,\d+$' then replace(trim({{ coluna }}::text), ',', '.')::{{ tipo }}
        when trim({{ coluna }}::text) ~ '^-?\d{1,3}(\.\d{3})+(,\d+)?$'
            then replace(replace(trim({{ coluna }}::text), '.', ''), ',', '.')::{{ tipo }}
        when trim({{ coluna }}::text) ~ '^-?\d+(\.\d+)?[eE]-?\d+$' then trim({{ coluna }}::text)::double precision::{{ tipo }}
    end
{%- endmacro %}


{% macro ts_data(coluna) -%}
    {#- "2024-12-30 00:00:00" (xlsx), "30/12/2024" e sentinelas como "00/01/1900" (nulo) -#}
    case
        when trim({{ coluna }}::text) ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
             and left(trim({{ coluna }}::text), 4) > '1901'
            then left(trim({{ coluna }}::text), 10)::date
        when trim({{ coluna }}::text) ~ '^(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/\d{4}$'
             and right(trim({{ coluna }}::text), 4) > '1901'
            then to_date(trim({{ coluna }}::text), 'DD/MM/YYYY')
    end
{%- endmacro %}


{% macro ts_dt_referencia_arquivo(coluna_arquivo) -%}
    {#- Competência da remessa a partir do nome do arquivo:
        ..._202607... (aaaamm) -> 2026-07-31;  ...-2026-09-02... (aaaa-mm-dd) -> 2026-09-02 -#}
    case
        when {{ coluna_arquivo }} ~ '\d{4}-\d{2}-\d{2}'
            then (regexp_match({{ coluna_arquivo }}, '(\d{4}-\d{2}-\d{2})'))[1]::date
        when {{ coluna_arquivo }} ~ '(20\d{2})(0[1-9]|1[0-2])'
            then (
                to_date((regexp_match({{ coluna_arquivo }}, '(20\d{2}(0[1-9]|1[0-2]))'))[1], 'YYYYMM')
                + interval '1 month' - interval '1 day'
            )::date
    end
{%- endmacro %}


{% macro bnz_seed_texto(nome_seed) -%}
    {#- Bronze de seed: o mesmo conteúdo do seed curado, com toda coluna em texto. -#}
    {%- set relacao = ref(nome_seed) -%}
    {%- if execute -%}
        {%- set colunas = adapter.get_columns_in_relation(relacao) -%}
    {%- else -%}
        {%- set colunas = [] -%}
    {%- endif %}
    select
        {%- for c in colunas %}
        cast({{ adapter.quote(c.name) }} as text) as {{ adapter.quote(c.name) }}{{ "," if not loop.last }}
        {%- else %}
        *
        {%- endfor %}
    from {{ relacao }}
{%- endmacro %}
