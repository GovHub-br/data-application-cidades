{#
    Resolve uma fonte declarada em `sources.yml` para a chamada `read_parquet`
    correspondente no MinIO.

    Por que existe: os caminhos dos parquets do lake estavam repetidos como
    string literal dentro de cada model bronze. Isso deixava a linhagem do dbt
    começar na bronze (tudo acima ficava invisível) e espalhava o caminho por
    28 arquivos — trocar um bucket ou um prefixo virava caça ao literal.

    Aqui o caminho fica declarado num lugar só (`sources.yml`, campo
    `meta.caminho`) e a chamada a `source()` registra a dependência no grafo,
    então a linhagem passa a mostrar a origem.

    Uso no model bronze:
        select * from {{ fonte_lake('ibge_sinapi') }}
#}
{% macro fonte_lake(
    nome_tabela,
    nome_fonte='lake_staging',
    filename=false,
    union_by_name=false
) %}
    {#- registra a dependência no grafo do dbt (o Relation em si não é usado:
        o dado é parquet no object storage, não uma tabela do Postgres) -#}
    {%- set _ = source(nome_fonte, nome_tabela) -%}

    {%- set bucket = var('lake_bucket', 'data-lake-mcid') -%}
    {#- raiz do lake: o MinIO por padrão; um diretório local nos testes do macro -#}
    {%- set root = var('lake_root', 's3://' ~ bucket) -%}

    {#- `graph` só está populado na fase de execução; no primeiro passe (parse)
        ele vem vazio, então o lookup precisa ficar atrás do guard `execute`,
        senão todo model quebra na análise com "não tem meta.caminho". -#}
    {%- if execute -%}
        {%- set fonte = namespace(meta=none) -%}
        {%- for no in graph.sources.values() -%}
            {%- if no.source_name == nome_fonte and no.name == nome_tabela -%}
                {%- set fonte.meta = no.meta -%}
            {%- endif -%}
        {%- endfor -%}
        {%- set meta = fonte.meta or {} -%}
        {%- set caminho = meta.get('caminho') -%}

        {%- if not caminho -%}
            {{ exceptions.raise_compiler_error(
                "fonte_lake: '" ~ nome_tabela ~ "' não tem meta.caminho em sources.yml") }}
        {%- endif -%}

        {%- set load_mode = meta.get('load_mode') -%}
        {%- set keys = meta.get('keys') or [] -%}
        {%- if keys is string -%}{%- set keys = [keys] -%}{%- endif -%}
        {#- as mesmas regras de ingestion.loaders.validate_load -#}
        {%- if load_mode == 'merge' and not keys -%}
            {{ exceptions.raise_compiler_error(
                "fonte_lake: '" ~ nome_tabela ~ "': merge exige keys em meta.keys") }}
        {%- elif load_mode in ('overwrite', 'append') and keys -%}
            {{ exceptions.raise_compiler_error(
                "fonte_lake: '" ~ nome_tabela ~ "': keys só no merge (load_mode: "
                ~ load_mode ~ ")") }}
        {%- endif -%}

        {%- if load_mode is none -%}
        {#- fonte legada: caminho é o arquivo (ou glob) a ler -#}
        read_parquet(
            '{{ root }}/{{ caminho }}'
            {%- if filename %}, filename => true{% endif -%}
            {%- if union_by_name %}, union_by_name => true{% endif -%}
        )
        {%- elif load_mode == 'overwrite' -%}
        {#- só a última ingestão completa, que a conversão publica em latest/ -#}
        read_parquet(
            '{{ root }}/{{ caminho }}/latest/*.parquet',
            filename => true,
            union_by_name => true
        )
        {%- elif load_mode == 'append' -%}
        {#- todas as ingestões empilhadas; 2*/*/ casa AAAA-MM-DD/HHMMSS e deixa o
            latest/ de fora (senão a última ingestão entraria duas vezes) -#}
        read_parquet(
            '{{ root }}/{{ caminho }}/2*/*/*.parquet',
            filename => true,
            union_by_name => true
        )
        {%- elif load_mode == 'merge' -%}
        {#- todas as ingestões; por arquivo + chave, vale a linha da ingestão mais
            recente (maior filename, porque AAAA-MM-DD/HHMMSS ordena como data).
            O arquivo entra na partição: séries em arquivos distintos (ipca, selic)
            não colidem na mesma chave. Chave sem ingestão nova fica com o último
            valor: deleção na fonte não se propaga. As chaves vão entre aspas porque
            a staging guarda o cabeçalho original. No pg_duckdb, a janela precisa
            rodar dentro de duckdb.query (fora dela as colunas são r['coluna']). -#}
        {%- set particao = ["regexp_extract(filename, '[^/]+$')"] -%}
        {%- for key in keys -%}
            {%- do particao.append('"' ~ key | replace('"', '""') ~ '"') -%}
        {%- endfor -%}
        {%- set consulta -%}
            select * exclude (_fonte_lake_linha)
            from (
                select
                    *,
                    row_number() over (
                        partition by {{ particao | join(', ') }}
                        order by filename desc
                    ) as _fonte_lake_linha
                from read_parquet(
                    '{{ root }}/{{ caminho }}/2*/*/*.parquet',
                    filename => true,
                    union_by_name => true
                )
            )
            where _fonte_lake_linha = 1
        {%- endset -%}
        {%- if target.type == 'postgres' -%}
        duckdb.query($fonte_lake${{ consulta }}$fonte_lake$)
        {%- else -%}
        ({{ consulta }})
        {%- endif -%}
        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "fonte_lake: '" ~ nome_tabela ~ "' tem load_mode desconhecido: '"
                ~ load_mode ~ "' (válidos: overwrite, merge, append)") }}
        {%- endif -%}
    {%- else -%}
        {#- placeholder só para o parse; nunca chega a ser executado -#}
        read_parquet('s3://{{ bucket }}/__parse__')
    {%- endif -%}
{% endmacro %}
