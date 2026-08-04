{{ config(materialized='table') }}

with sintetico as (
    select * from {{ source('fgts', 'dados_abertos_mcmv_fgts_sintetico') }}
)

select
    txt_uf::varchar as uf,
    txt_municipio::varchar as municipio,
    cod_ibge::varchar as codigo_ibge,
    
    -- Tratar a data: ex '11/07/2025'
    to_date(nullif(data_referencia, ''), 'DD/MM/YYYY') as data_referencia,
    
    -- Tratar o ano: ex '2.009'
    replace(coalesce(nullif(num_ano_financiamento, ''), '0'), '.', '')::integer as ano_financiamento,
    
    -- Valores Financeiros (Formato PT-BR)
    replace(replace(coalesce(nullif(vlr_financiamento, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_financiamento,
    replace(replace(coalesce(nullif(vlr_subsidio, ''), '0'), '.', ''), ',', '.')::numeric(15, 2) as valor_subsidio,
    
    replace(coalesce(nullif(qtd_uh_financiadas, ''), '0'), '.', '')::integer as quantidade_uh_financiadas

from sintetico
