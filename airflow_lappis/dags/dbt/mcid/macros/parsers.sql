{#
    ===========================================================================
    Parsers de tipos simples -- Postgres e DuckDB
    ===========================================================================
    Todas as macros deste arquivo aceitam `engine`. O padrao e Postgres; models
    que leem parquet passam engine='duckdb', porque o pg_duckdb executa a query
    inteira no DuckDB e o dialeto Postgres nao vale la.

    Para trocar o padrao de um projeto inteiro, defina a var sql_engine.

    ATENCAO -- ONDE O RAMO 'duckdb' PODE SER USADO
    O SQL gerado pelo ramo duckdb so e valido DENTRO de um bloco
    duckdb.query($$ ... $$). Fora dali quem interpreta e o parser do Postgres,
    para quem nem a sintaxe do DuckDB nem suas funcoes existem -- so as que o
    pg_duckdb declara, como read_parquet. Verificado na pratica: literal de
    lista deu "syntax error at or near", try_strptime deu "function does not
    exist". Dentro do bloco e string, o Postgres nao olha, e vale DuckDB puro.
#}


{#
    Inteiro. Trata '' e a string literal 'None'.
    Postgres: nullif duplo antes do cast. DuckDB: try_cast absorve tudo.
#}
{% macro parse_int(column_name, engine=none) %}
    {%- set motor = engine or var('sql_engine', 'postgres') -%}
    {%- if motor == 'duckdb' -%}
        try_cast(trim({{ column_name }}) as integer)
    {%- else -%}
        nullif(nullif(trim({{ column_name }}), ''), 'None')::int
    {%- endif -%}
{% endmacro %}


{#
    Numerico com virgula decimal. Trata espacos, 'None' e o separador.
#}
{% macro parse_numeric(column_name, cast_type='numeric', engine=none) %}
    {%- set motor = engine or var('sql_engine', 'postgres') -%}
    {%- if motor == 'duckdb' -%}
        try_cast(
            replace(nullif(trim({{ column_name }}), 'None'), ',', '.')
            as {{ cast_type }}
        )
    {%- else -%}
        nullif(replace(nullif(trim({{ column_name }}), 'None'), ',', '.'), '')::{{ cast_type }}
    {%- endif -%}
{% endmacro %}


{#
    ---------------------------------------------------------------------------
    parse_timestamp(coluna, formatos=none, sentinela=..., engine=none)
    ---------------------------------------------------------------------------
    Data/hora a partir de texto, com tratamento da data sentinela.

    Existe porque a conversao de data estava escrita inline nos models, com
    to_timestamp e mascara Postgres -- foi exatamente ela que quebrou quando a
    query passou a ser executada pelo DuckDB.

    No Postgres usa a primeira mascara da lista, no formato de to_timestamp.
    No DuckDB usa try_strptime com a lista inteira de formatos, tentada em
    ordem: cobre variacoes sem custo e devolve NULL se nenhuma casar. Valido
    dentro de duckdb.query($$ ... $$).

        {{ parse_timestamp('c.dte_assinatura') }}
        {{ parse_timestamp('c.dte_assinatura', engine='duckdb') }}
#}
{% macro parse_timestamp(column_name, formatos=none, sentinela='1900-01-01 00:00:00', engine=none) %}
    {%- set motor = engine or var('sql_engine', 'postgres') -%}
    {%- set fmts = formatos or [
        '%m/%d/%y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%d'
    ] -%}
    {%- if motor == 'duckdb' -%}
        try_strptime(
            nullif(trim({{ column_name }}), '{{ sentinela }}'),
            [{% for f in fmts %}'{{ f }}'{% if not loop.last %}, {% endif %}{% endfor %}]
        )
    {%- else -%}
        {%- set mascara_pg = {
            '%m/%d/%y %H:%M:%S': 'MM/DD/YY HH24:MI:SS',
            '%Y-%m-%d %H:%M:%S': 'YYYY-MM-DD HH24:MI:SS',
            '%d/%m/%Y %H:%M:%S': 'DD/MM/YYYY HH24:MI:SS',
            '%Y-%m-%d': 'YYYY-MM-DD'
        }.get(fmts[0], 'MM/DD/YY HH24:MI:SS') -%}
        to_timestamp(
            nullif(trim({{ column_name }}), '{{ sentinela }}'),
            '{{ mascara_pg }}'
        )
    {%- endif -%}
{% endmacro %}
