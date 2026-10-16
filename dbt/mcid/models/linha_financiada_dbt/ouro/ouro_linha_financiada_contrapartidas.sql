{{ config(materialized='table') }}

select
    segmento_linha_financiada,
    fonte_recurso,
    tipo_contrapartida,
    fonte_informacao,
    ic_valor_informado,
    count(*) as quantidade_contratos,
    sum(coalesce(valor_contrapartida, 0)) as valor_contrapartida,
    100.0 * count(*) filter (where ic_valor_informado)
        / nullif(sum(count(*)) over (), 0) as percentual_cobertura_global,
    max(ressalva_cobertura) as ressalva_cobertura,
    current_timestamp as dt_gold
from {{ ref('prata_linha_financiada_contrapartida') }}
group by 1, 2, 3, 4, 5
