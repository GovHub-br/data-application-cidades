{#
    ===========================================================================
    Silver -- execucao fisica das obras
    ===========================================================================
    PADROES EXERCITADOS AQUI
      - FLAG POR EXISTENCIA DE JOIN: is_paralisada nasce de um left join que
        pode nao casar -- padrao muito comum nas silvers do projeto
      - MULTIPLAS MASCARAS DE DATA no mesmo model: a origem grava dt_ultimo_bm
        como YYYY-MM-DD e dt_previsao_conclusao_objeto como DD/MM/YYYY. E o
        caso que melhor justifica a lista de formatos do parse_timestamp: uma
        chamada so resolve as duas, e ainda absorve variacao futura
      - cast de data (nao timestamp) na projecao externa
      - dois percentuais pela macro financeira

    Recorte fiel do silver_fgts_execucao_obras de producao.

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
    {{ silver_codigo("r['cod_contrato']::varchar") }}::varchar               as cod_contrato,
    {{ silver_codigo("r['cod_situacao_obra']::varchar") }}::varchar          as cod_situacao_obra,
    {{ silver_texto("r['situacao_obra_descricao']::varchar") }}::varchar    as situacao_obra_descricao,

    {{ silver_medida("r['percentual_previsto']::numeric(15, 2)") }}          as percentual_previsto,
    {{ silver_medida("r['percentual_realizado']::numeric(15, 2)") }}         as percentual_realizado,
    {{ silver_texto("r['ano_mes_avaliacao']::varchar") }}::varchar          as ano_mes_avaliacao,

    {{ silver_booleano("r['is_paralisada']::boolean") }}                    as is_paralisada,
    {{ silver_medida_inteira("r['dias_sem_evolucao']::integer") }}                 as dias_sem_evolucao,
    {{ silver_texto("r['paralisacao_situacao_atual']::varchar") }}::varchar as paralisacao_situacao_atual,
    {{ silver_timestamp("r['dt_ultimo_bm']::timestamp") }}                       as dt_ultimo_bm,
    {{ silver_timestamp("r['dt_previsao_conclusao_objeto']::timestamp") }}       as dt_previsao_conclusao_objeto,

    -- dt_ingest fica nulo ate a auditoria de staging ser propagada
    null::timestamptz  as dt_ingest,
    current_timestamp  as dt_silver

from duckdb.query(
$DBTSTG$

    with execucao as (
        select * from {{ staging_parquet('fgts_staging', 'execucoes_obras') }}
    ),
    paralisadas as (
        select * from {{ staging_parquet('fgts_staging', 'operacoes_paralisadas') }}
    ),
    situacao as (
        select * from {{ staging_parquet('fgts_staging', 'situacao_da_obra') }}
    )

    select
        e.cod_contrato,
        e.cod_situacao_obra,
        coalesce(s.situacao_da_obra, 'Não Informado') as situacao_obra_descricao,

        {{ parse_financial_value('e.prc_prev_acum_mes', engine='duckdb') }} as percentual_previsto,
        {{ parse_financial_value('e.prc_real_acum_mes', engine='duckdb') }} as percentual_realizado,
        e.dte_ano_mes_avaliacao as ano_mes_avaliacao,

        -- flag por existencia: o left join so casa para contratos paralisados
        p.cod_contrato is not null as is_paralisada,

        {{ parse_int('p.dias_sem_evolucao', engine='duckdb') }} as dias_sem_evolucao,
        p.situacao_atual as paralisacao_situacao_atual,

        -- mascaras diferentes na mesma tabela; a lista cobre as duas
        {{ parse_timestamp('p.dt_ultimo_bm', engine='duckdb') }} as dt_ultimo_bm,
        {{ parse_timestamp('p.dt_previsao_conclusao_objeto', engine='duckdb') }} as dt_previsao_conclusao_objeto

    from execucao e
    left join situacao s    on e.cod_situacao_obra = s.codigo
    left join paralisadas p on e.cod_contrato = p.cod_contrato

    -- chave ausente nao entra na silver (padrao, regra 1)
    where e.cod_contrato is not null
      and trim(e.cod_contrato) <> ''

$DBTSTG$
) r
