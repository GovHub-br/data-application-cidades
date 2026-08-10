{#
    ===========================================================================
    Silver -- posicoes mensais dos empreendimentos
    ===========================================================================
    PADROES EXERCITADOS AQUI
      - WINDOW FUNCTION: row_number() over (partition by ... order by ...)
        para marcar a posicao mais recente de cada empreendimento
      - CTE encadeada dentro do bloco: ranqueia primeiro, projeta depois
      - varias colunas de data com a mesma mascara
      - boolean derivado de comparacao

    Window function e um dos pontos em que valia confirmar que o desenho
    aguenta: como tudo roda dentro do DuckDB, funciona igual ao Postgres --
    e sem o custo de trazer o historico inteiro para o banco antes de filtrar.

    Recorte fiel do silver_fgts_empreendimentos_posicoes de producao.

    PADRAO DA CAMADA SILVER
    Atributo, boolean e contagem nunca saem nulos. MEDIDA e DATA preservam o
    nulo -- zero em medida distorce media e minimo sem dar erro, e a decisao de
    exibir zero e da gold. Timestamp e sempre timestamptz, interpretado no fuso
    de origem. As macros de macros/silver_padroes.sql sao aplicadas na projecao
    externa. Ver docs/padrao-camada-silver.md.
#}

{{
    config(
        enabled=var('fgts_poc_enabled', false),
        materialized='table',
        schema='fgts_poc',
        tags=['fgts_poc'],
        pre_hook='set duckdb.force_execution = true'
    )
}}

select
    {{ silver_codigo("r['cod_empreendimento']::varchar") }}::varchar    as cod_empreendimento,
    {{ silver_texto("r['ano_mes_posicao']::varchar") }}::varchar       as ano_mes_posicao,
    {{ silver_medida("r['percentual_executado']::numeric(15, 2)") }}    as percentual_executado,
    {{ silver_timestamp("r['data_inicio']::timestamp") }}                   as data_inicio,
    {{ silver_timestamp("r['data_termino']::timestamp") }}                  as data_termino,
    {{ silver_timestamp("r['data_inauguracao']::timestamp") }}              as data_inauguracao,
    {{ silver_booleano("r['is_posicao_atual']::boolean") }}            as is_posicao_atual,

    -- dt_ingest fica nulo ate a auditoria de staging ser propagada
    null::timestamptz  as dt_ingest,
    current_timestamp  as dt_silver

from duckdb.query(
$DBTSTG$

    with posicoes as (
        select * from {{ staging_parquet('fgts_staging', 'empreendimentos_posicoes') }}
    ),

    ranqueadas as (
        select
            cod_empreendimento,
            dte_ano_mes as ano_mes_posicao,
            {{ parse_financial_value('prc_obra_executada_ult', engine='duckdb') }} as percentual_executado,

            {{ parse_timestamp('dt_inicio', engine='duckdb') }}      as data_inicio,
            {{ parse_timestamp('dt_termino', engine='duckdb') }}     as data_termino,
            {{ parse_timestamp('dt_inauguracao', engine='duckdb') }} as data_inauguracao,

            row_number() over (
                partition by cod_empreendimento
                order by dte_ano_mes desc
            ) as rn
        from posicoes
        -- chave ausente nao entra na silver (padrao, regra 1)
        where cod_empreendimento is not null
          and trim(cod_empreendimento) <> ''
    )

    select
        cod_empreendimento,
        ano_mes_posicao,
        percentual_executado,
        data_inicio,
        data_termino,
        data_inauguracao,
        rn = 1 as is_posicao_atual
    from ranqueadas

$DBTSTG$
) r
