{#
    Devolve só o caminho s3:// de uma fonte do lake declarada em sources.yml (meta.caminho),
    registrando a dependência no grafo como fonte_lake faz. Serve para bronzes que montam o
    read_parquet com opções próprias (glob de remessas, hive_partitioning etc.), como as do
    Trabalho Social (bnz_ts_staging).

    Uso: read_parquet('{{ caminho_fonte_lake("mcmv_ts_remessas_historico") }}', filename => true)
#}
{% macro caminho_fonte_lake(nome_tabela, nome_fonte='lake_staging') -%}
    {#- registra a dependência no grafo do dbt (o Relation em si não é usado:
        o dado é parquet no object storage, não uma tabela do Postgres) -#}
    {%- set _ = source(nome_fonte, nome_tabela) -%}

    {%- set bucket = var('lake_bucket', 'data-lake-mcid') -%}

    {#- `graph` só está populado na fase de execução; no primeiro passe (parse)
        ele vem vazio, então o lookup precisa ficar atrás do guard `execute`,
        senão todo model quebra na análise com "não tem meta.caminho". -#}
    {%- if execute -%}
        {%- set caminho = namespace(valor=none) -%}
        {%- for no in graph.sources.values() -%}
            {%- if no.source_name == nome_fonte and no.name == nome_tabela -%}
                {%- set caminho.valor = no.meta.get('caminho') -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if not caminho.valor -%}
            {{ exceptions.raise_compiler_error(
                "caminho_fonte_lake: '" ~ nome_tabela ~ "' não tem meta.caminho em sources.yml") }}
        {%- endif -%}

        {%- do return('s3://' ~ bucket ~ '/' ~ caminho.valor) -%}
    {%- else -%}
        {#- placeholder só para o parse; nunca chega a ser executado -#}
        {%- do return('s3://' ~ bucket ~ '/__parse__') -%}
    {%- endif -%}
{%- endmacro %}
