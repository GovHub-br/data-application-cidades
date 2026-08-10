{#
    ---------------------------------------------------------------------------
    staging_parquet(source_name, table_name)
    ---------------------------------------------------------------------------
    Devolve a expressao de leitura de um parquet da staging no MinIO, e -- o
    ponto principal -- registra a dependencia no manifest do dbt.

    Uso, DENTRO de um bloco duckdb.query -- que e onde a leitura deve ficar,
    porque so ali vale SQL DuckDB de verdade:

        from duckdb.query($DBTSTG$
            with contratos as (
                select * from {{ staging_parquet('fgts_staging', 'contratos') }}
            )
            select ... from contratos c
        $DBTSTG$) r

    compila para:

        read_parquet('s3://data-lake-mcid/staging/fgts_...parquet')

    A chamada funciona igual dentro do bloco: para o dbt e so texto renderizado
    antes de o SQL sair, entao a dependencia continua registrada no manifest.

    ---------------------------------------------------------------------------
    Por que existe
    ---------------------------------------------------------------------------
    O manifest do dbt so conhece dependencias declaradas por ref() e source().
    Um read_parquet() escrito a mao no model e invisivel: o OpenMetadata monta a
    linhagem a partir do manifest, entao o model apareceria orfao, sem nenhuma
    origem. A chamada a source() aqui dentro resolve isso sem criar camada
    nenhuma -- source e no de metadado, nao vira model, nem objeto no banco,
    nem task no Cosmos.

    De quebra, o caminho do arquivo passa a morar no sources.yml em vez de
    hardcoded em cada model: renomear um objeto no bucket vira correcao em um
    lugar so.

    ---------------------------------------------------------------------------
    Configuracao (em models/sources.yml)
    ---------------------------------------------------------------------------
        - name: fgts_staging
          meta:
            prefixo: "s3://data-lake-mcid/staging"
          tables:
            - name: contratos
              meta:
                arquivo: fgts_canal_tab_ao_1_contratos_fgts.parquet

    Sem meta.arquivo, o nome do objeto e "<name>.parquet".
    A var fgts_staging_prefix, se definida, tem precedencia sobre meta.prefixo.

    ---------------------------------------------------------------------------
    Como conferir que a linhagem foi registrada
    ---------------------------------------------------------------------------
        dbt ls --select source:fgts_staging+ --vars '{fgts_poc_enabled: true}'

    Os models que consomem a staging tem que aparecer na saida. Se nao
    aparecerem, a dependencia nao foi capturada -- nesse caso acrescente no
    topo do model, como comentario:

        -- depends_on: {{ source('fgts_staging', 'contratos') }}
#}

{% macro staging_parquet(source_name, table_name) %}

    {#- efeito colateral proposital: registra a aresta no manifest -#}
    {%- set _ignorado = source(source_name, table_name) -%}

    {%- if not execute -%}
        {#- em tempo de parse o grafo ainda nao existe; o SQL nao e executado -#}
        {%- do return("read_parquet('parse_time_placeholder.parquet')") -%}
    {%- endif -%}

    {%- set achados = [] -%}
    {%- for node in graph.sources.values() -%}
        {%- if node.source_name == source_name and node.name == table_name -%}
            {%- do achados.append(node) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if achados | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "staging_parquet: source '" ~ source_name ~ "." ~ table_name ~
            "' nao encontrado. Declare em models/sources.yml."
        ) }}
    {%- endif -%}

    {%- set node = achados[0] -%}
    {%- set meta_tabela = node.meta or {} -%}
    {%- set meta_source = node.source_meta or {} -%}

    {%- set prefixo = var(
        'fgts_staging_prefix',
        meta_source.get('prefixo', 's3://data-lake-mcid/staging')
    ) -%}
    {%- if prefixo.endswith('/') -%}
        {%- set prefixo = prefixo[:-1] -%}
    {%- endif -%}

    {%- set arquivo = meta_tabela.get('arquivo', table_name ~ '.parquet') -%}

    {{- "read_parquet('" ~ prefixo ~ "/" ~ arquivo ~ "')" -}}

{% endmacro %}
