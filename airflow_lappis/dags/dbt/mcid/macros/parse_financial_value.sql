{#
    ===========================================================================
    parse_financial_value(coluna, engine=none)
    ===========================================================================
    Converte texto para numeric(15,2)/decimal(15,2) cobrindo os formatos que
    aparecem nas bases: brasileiro com milhar ("34.679.700,00"), so com virgula
    decimal ("34679700,00"), GFAR com zeros a esquerda ("0000000034679700,00")
    e ja numerico com ponto ("34679700.00").

    DOIS DIALETOS
    -------------
    O pg_duckdb nao faz execucao hibrida: uma query que referencia um parquet e
    executada INTEIRA pelo DuckDB. Como o operador `~` e sintaxe Postgres, a
    versao original desta macro quebra nesses models. Por isso ela agora emite
    SQL nos dois dialetos.

        {{ parse_financial_value('c.vlr_contratado') }}                    -- Postgres (padrao)
        {{ parse_financial_value('c.vlr_contratado', engine='duckdb') }}   -- models que leem parquet

    O padrao e Postgres de proposito: 101 chamadas espalhadas por fgts_dbt,
    entidades_dbt e empreendimento_far_dbt continuam lendo tabelas do
    __dados_brutos e nao devem mudar de comportamento.

    Para trocar o padrao de um projeto inteiro, defina a var sql_engine.

    DIFERENCA DE COMPORTAMENTO ENTRE OS DIALETOS
    --------------------------------------------
    No Postgres e preciso TESTAR o formato antes de converter, porque um cast
    invalido aborta a query -- dai o encadeamento de `case when`, uma clausula
    por formato conhecido.

    No DuckDB da para TENTAR converter: try_cast devolve NULL em vez de
    estourar, o que dispensa testar o formato antes.

    ATENCAO -- ONDE O RAMO 'duckdb' PODE SER USADO
    O SQL que ele gera so e valido DENTRO de um bloco duckdb.query($$ ... $$).
    Fora dali quem le e o parser do Postgres, e nem a sintaxe try_cast(x as
    tipo) nem funcoes do DuckDB existem para ele -- so as que o pg_duckdb
    declara, como read_parquet. Verificado na pratica: literal de lista deu
    "syntax error", try_strptime deu "function does not exist".

    A consequencia pratica e que um valor irreconhecivel vira 0.00 no DuckDB,
    enquanto no Postgres cairia no `else` e poderia abortar. E fail-soft --
    desejavel em ingestao, mas mascara dado sujo. Vale monitorar a proporcao de
    zeros na coluna resultante.
#}

{% macro parse_financial_value(column_name, engine=none) %}
    {%- set motor = engine or var('sql_engine', 'postgres') -%}
    {%- if motor == 'duckdb' -%}
        {{ _parse_financial_value_duckdb(column_name) }}
    {%- else -%}
        {{ _parse_financial_value_postgres(column_name) }}
    {%- endif -%}
{% endmacro %}


{% macro _parse_financial_value_postgres(column_name) %}

    case
        when {{ column_name }} is null or trim({{ column_name }}) = '' or trim({{ column_name }}) = 'None'
        then 0.00::numeric(15, 2)

        when {{ column_name }} like '%NaN%'
        then 0.00::numeric(15, 2)
        -- Formato GFAR: "0000000034679700,00" (zeros à esquerda, vírgula decimal)
        when {{ column_name }} ~ '^0+\d+,\d+$'
        then replace(
            ltrim({{ column_name }}, '0'),
            ',', '.'
        )::numeric(15, 2)
        -- Formato brasileiro padrão: "34.679.700,00" (ponto milhar, vírgula decimal)
        when {{ column_name }} like '%,%' and {{ column_name }} like '%.%'
        then replace(
            replace(coalesce({{ column_name }}, '0'), '.', ''),
            ',', '.'
        )::numeric(15, 2)
        -- Formato com apenas vírgula decimal: "34679700,00"
        when {{ column_name }} like '%,%'
        then replace(
            coalesce({{ column_name }}, '0'),
            ',', '.'
        )::numeric(15, 2)
        -- Formato já numérico: "34679700.00"
        else coalesce(nullif(trim({{ column_name }}), ''), '0')::numeric(15, 2)
    end

{% endmacro %}


{#
    Somente funcoes e cast `::` -- nada de sintaxe exclusiva do DuckDB, que o
    parser do Postgres rejeitaria antes do pushdown.
#}
{#
    Valido apenas dentro de duckdb.query($$ ... $$).
#}
{% macro _parse_financial_value_duckdb(column_name, se_nulo='0.00') %}

    coalesce(
        try_cast(
            case
                when contains({{ column_name }}, ',')
                    then replace(replace(trim({{ column_name }}), '.', ''), ',', '.')
                else trim({{ column_name }})
            end
            as decimal(15, 2)
        ),
        {{ se_nulo }}
    )

{% endmacro %}
