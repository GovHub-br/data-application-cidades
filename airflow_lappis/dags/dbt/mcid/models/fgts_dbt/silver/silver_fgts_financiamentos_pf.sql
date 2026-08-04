{{ config(materialized='table') }}

with cci as (
    select * from {{ source('fgts', 'fgts_canal_tab_cci_cca_cci_analitico') }}
),
cca as (
    select * from {{ source('fgts', 'fgts_canal_tab_cci_cca_cca_analitico') }}
),
pro_cotista as (
    select * from {{ source('fgts', 'fgts_canal_tab_cci_cca_pro_cotista') }}
),
faixas_renda as (
    select * from {{ source('fgts', 'fgts_canal_tdom_cci_cca_faixa_de_renda') }}
)

select
    c.numero_do_contrato::varchar,
    c.faixa_de_renda_codigo::varchar,
    {{ parse_financial_value('c.vlr_do_financiamento') }} as valor_financiamento,
    'CCI'::varchar as origem_pf,
    coalesce(f.faixa_de_renda, 'Não Informado')::varchar as faixa_renda_descricao
from cci c
left join faixas_renda f on c.faixa_de_renda_codigo = f.codigo

union all

select
    a.numero_do_contrato::varchar,
    a.faixa_de_renda_codigo::varchar,
    {{ parse_financial_value('a.vlr_do_financiamento') }} as valor_financiamento,
    'CCA'::varchar as origem_pf,
    coalesce(f.faixa_de_renda, 'Não Informado')::varchar as faixa_renda_descricao
from cca a
left join faixas_renda f on a.faixa_de_renda_codigo = f.codigo

union all

select
    p.numero_do_contrato::varchar,
    p.faixa::varchar as faixa_de_renda_codigo,
    {{ parse_financial_value('p.vlr_do_financiameto_bruto') }} as valor_financiamento,
    'PRO-COTISTA'::varchar as origem_pf,
    coalesce(f.faixa_de_renda, 'Não Informado')::varchar as faixa_renda_descricao
from pro_cotista p
left join faixas_renda f on p.faixa = f.codigo
