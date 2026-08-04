{{ config(materialized='table') }}

with execucao as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_2_tab_execucoes_obras') }}
),
paralisadas as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_2_operacoes_paralisadas_fgts_setorpublico') }}
),
situacao_obra as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_situacao_da_obra') }}
)

select
    e.cod_contrato::varchar,
    e.cod_situacao_obra::varchar,
    coalesce(s.situacao_da_obra, 'Não Informado')::varchar as situacao_obra_descricao,
    
    {{ parse_financial_value('e.prc_prev_acum_mes') }} as percentual_previsto,
    {{ parse_financial_value('e.prc_real_acum_mes') }} as percentual_realizado,
    e.dte_ano_mes_avaliacao::varchar as ano_mes_avaliacao,
    
    -- Flag de Paralisação
    case when p.cod_contrato is not null then true else false end::boolean as is_paralisada,
    
    -- Detalhes da Paralisação
    p.dias_sem_evolucao::integer,
    p.situacao_atual::varchar as paralisacao_situacao_atual,
    to_date(nullif(p.dt_ultimo_bm, '1900-01-01 00:00:00'), 'YYYY-MM-DD') as dt_ultimo_bm,
    to_date(nullif(p.dt_previsao_conclusao_objeto, '1900-01-01 00:00:00'), 'DD/MM/YYYY') as dt_previsao_conclusao_objeto
    
from execucao e
left join situacao_obra s on e.cod_situacao_obra = s.codigo
left join paralisadas p on e.cod_contrato = p.cod_contrato
