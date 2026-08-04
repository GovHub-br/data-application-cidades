{{ config(materialized='table') }}

with contratacao as (
    select * from {{ source('fgts', 'fgts_site_contratacao_diaria') }}
)

select
    programa::varchar,
    mes_ano::varchar,
    uf::varchar,
    area::varchar,
    pj_pf::varchar as tipo_operacao,
    
    {{ parse_financial_value('valor_do_emprestimo') }} as valor_emprestimo,
    {{ parse_int('numero_de_unidades') }} as numero_unidades,
    {{ parse_financial_value('empregos_gerados') }} as empregos_gerados,
    {{ parse_financial_value('populacao_beneficiada') }} as populacao_beneficiada

from contratacao
