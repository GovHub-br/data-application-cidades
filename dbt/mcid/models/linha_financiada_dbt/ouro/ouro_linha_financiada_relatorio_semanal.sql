{{ config(materialized='table') }}

select
    date_trunc('week', data_contratacao)::date as semana_referencia,
    fonte_recurso,
    segmento_linha_financiada,
    carteira,
    faixa_codigo,
    modalidade,
    tipo_imovel,
    ic_classe_media,
    ic_mcmv_cidades,
    ic_pro_moradia,
    count(*) as quantidade_contratos,
    sum(coalesce(valor_financiamento, 0)) as valor_financiamento,
    sum(coalesce(valor_desconto_fgts, 0) + coalesce(valor_desconto_ogu, 0)) as valor_descontos,
    sum(coalesce(valor_contrapartida_informada, 0)) as valor_contrapartida_informada,
    current_timestamp as dt_gold
from {{ ref('ouro_linha_financiada_base_agregada') }}
where data_contratacao is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
