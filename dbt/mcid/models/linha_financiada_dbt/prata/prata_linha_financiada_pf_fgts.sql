{{ config(materialized='table') }}

-- Recorte analítico e sem identificadores pessoais da Base PF/FGTS.
-- Mantido separado de CCI/CCA porque as bases têm critérios de recorte
-- distintos e sua soma produziria dupla contagem dos indicadores oficiais.

select
    try_cast(dt_assinatura::text as date) as data_contratacao,
    trim(faixa::text) as faixa_codigo,
    trim(tp_orcamento::text) as tipo_orcamento,
    trim(tpimovel::text) as tipo_imovel,
    try_cast(replace(vlr_emprestimo::text, ',', '.') as numeric) as valor_financiamento,
    current_timestamp as dt_silver
from {{ ref('bronze_geavo_fgts_pf') }}
where try_cast(dt_assinatura::text as date) is not null
