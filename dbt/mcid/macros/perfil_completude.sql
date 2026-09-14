{#
    Gera uma linha por coluna de cada model informado, com a contagem de
    nulos e o % de completude.

    Existe porque "a base está completa?" não é respondível olhando model por
    model — e porque completude que ninguém mede vira surpresa no dashboard.
    O resultado é materializado em `gld_qualidade_completude`, então dá pra
    acompanhar a evolução e pendurar teste em cima.

    Usa o catálogo do Postgres (`information_schema`) pra descobrir as
    colunas em tempo de compilação, e monta uma consulta com UMA varredura
    POR TABELA — nunca por coluna. `count(*)` e `count(coluna)` de todas as
    colunas de uma tabela saem juntos num `select` só, dentro de uma CTE; o
    resto da consulta só desempilha essa linha única em N linhas de saída,
    sem tocar a tabela de novo (o Postgres materializa uma CTE referenciada
    mais de uma vez, então o desempilhamento não paga custo de I/O extra).

    Antes disso, cada coluna era a sua própria varredura: uma tabela de 50
    colunas era lida 50 vezes na mesma instrução. Numa execução real (2026-09)
    isso gerou 1,2 MB de SQL com 2.258 subconsultas e derrubou o Postgres do
    DW por falta de memória.

    `excluir_prefixo` tira as tabelas cujo nome começa com o prefixo — usado
    para excluir a bronze, que não é o alvo de completude (ela espelha a
    origem, e pode legitimamente ter coluna toda nula).

    `relacoes` recebe a lista de `{schema, tabela}` do `relacoes_do_produto()` e
    substitui o filtro por schema. É o que a arquitetura de três schemas exige:
    `prata` e `ouro` são compartilhados entre os domínios, então "todo o schema"
    deixou de ser o mesmo que "este produto".
#}
{% macro perfil_completude(schemas=none, excluir_prefixo=none, relacoes=none) %}

    {%- if execute and (relacoes is none or relacoes) -%}
        {%- set consulta -%}
            select table_schema, table_name, column_name
            from information_schema.columns
            where
            {%- if relacoes is not none %}
                {#- recorte exato vindo do grafo: par (schema, tabela) -#}
                (table_schema, table_name) in (
                {%- for r in relacoes -%}
                    ('{{ r["schema"] }}', '{{ r["tabela"] }}'){{ ", " if not loop.last }}
                {%- endfor -%}
                )
            {%- else %}
                table_schema in (
                {%- for s in schemas | unique -%}'{{ s }}'{{ ", " if not loop.last }}{%- endfor -%}
                )
                {%- if excluir_prefixo %}
                and table_name not like '{{ excluir_prefixo }}%'
                {%- endif %}
            {%- endif %}
              and column_name not in ('dt_ingest', '_source_file', '_ingested_at', '_source_hash')
            order by table_schema, table_name, ordinal_position
        {%- endset -%}
        {%- set linhas = run_query(consulta).rows -%}

        {#- agrupa as colunas por tabela, na ordem em que apareceram -#}
        {%- set tabelas = [] -%}
        {%- set colunas_por_tabela = {} -%}
        {%- for linha in linhas -%}
            {%- set chave = linha[0] ~ '.' ~ linha[1] -%}
            {%- if chave not in colunas_por_tabela -%}
                {%- do tabelas.append(chave) -%}
                {%- do colunas_por_tabela.update(
                    {chave: {"schema": linha[0], "tabela": linha[1], "colunas": []}}) -%}
            {%- endif -%}
            {%- do colunas_por_tabela[chave]["colunas"].append(linha[2]) -%}
        {%- endfor -%}

        {%- if not tabelas -%}
            select null::text as schema_dado, null::text as model, null::text as coluna,
                   null::bigint as linhas, null::bigint as preenchidas,
                   null::numeric as completude, current_timestamp as medido_em
            where false
        {%- else -%}
            {%- set ctes = [] -%}
            {%- set blocos = [] -%}
            {%- for chave in tabelas -%}
                {%- set idx = loop.index -%}
                {%- set info = colunas_por_tabela[chave] -%}
                {%- set contagens = [] -%}
                {%- for c in info["colunas"] -%}
                    {%- do contagens.append('count("' ~ c ~ '") as c' ~ loop.index0) -%}
                {%- endfor -%}
                {%- do ctes.append(
                    "cte_" ~ idx ~ " as (select count(*) as linhas, "
                    ~ (contagens | join(", "))
                    ~ " from " ~ info["schema"] ~ '."' ~ info["tabela"] ~ '")'
                ) -%}
                {%- for c in info["colunas"] -%}
                    {%- do blocos.append(
                        "select '" ~ info["schema"] ~ "'::text as schema_dado, '"
                        ~ info["tabela"] ~ "'::text as model, '" ~ c ~ "'::text as coluna, "
                        ~ "cte_" ~ idx ~ ".linhas, cte_" ~ idx ~ ".c" ~ loop.index0
                        ~ " as preenchidas, "
                        ~ "case when cte_" ~ idx ~ ".linhas = 0 then null else round(cte_"
                        ~ idx ~ ".c" ~ loop.index0 ~ "::numeric / cte_" ~ idx ~ ".linhas, 4) end"
                        ~ " as completude, current_timestamp as medido_em from cte_" ~ idx
                    ) -%}
                {%- endfor -%}
            {%- endfor -%}

            with
            {{ ctes | join(",\n") }}
            {{ blocos | join("\nunion all\n") }}
        {%- endif -%}
    {%- else -%}
        select null::text as schema_dado, null::text as model, null::text as coluna,
               null::bigint as linhas, null::bigint as preenchidas,
               null::numeric as completude, current_timestamp as medido_em
        where false
    {%- endif -%}

{% endmacro %}
