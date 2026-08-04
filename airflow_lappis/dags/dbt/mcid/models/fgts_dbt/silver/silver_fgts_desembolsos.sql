{{ config(materialized='table') }}

with desembolsos as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_2_tab_desembolsos_fgts') }}
)

select
    cod_contrato::varchar,
    dte_ano::integer as ano_referencia,
    dte_mes_ref::integer as mes_referencia,
    
    {{ parse_financial_value('vlr_liberado') }} as valor_liberado
    
from desembolsos
