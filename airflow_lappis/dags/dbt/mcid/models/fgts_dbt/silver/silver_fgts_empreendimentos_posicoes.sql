{{ config(materialized='table') }}

with posicoes as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_1_tab_empreendimentos_posicoes') }}
),
posicoes_ranqueadas as (
    select 
        cod_empreendimento::varchar,
        dte_ano_mes::varchar as ano_mes_posicao,
        {{ parse_financial_value('prc_obra_executada_ult') }} as percentual_executado,
        
        to_timestamp(nullif(dt_inicio, '1900-01-01 00:00:00'), 'MM/DD/YY HH24:MI:SS') as data_inicio,
        to_timestamp(nullif(dt_termino, '1900-01-01 00:00:00'), 'MM/DD/YY HH24:MI:SS') as data_termino,
        to_timestamp(nullif(dt_inauguracao, '1900-01-01 00:00:00'), 'MM/DD/YY HH24:MI:SS') as data_inauguracao,
        
        row_number() over (partition by cod_empreendimento order by dte_ano_mes desc) as rn
    from posicoes
)

select
    cod_empreendimento,
    ano_mes_posicao,
    percentual_executado,
    data_inicio,
    data_termino,
    data_inauguracao,
    case when rn = 1 then true else false end::boolean as is_posicao_atual
from posicoes_ranqueadas
