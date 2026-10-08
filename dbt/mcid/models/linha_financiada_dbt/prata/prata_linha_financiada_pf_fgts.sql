{{ config(materialized='table') }}

-- Recorte analítico e sem identificadores pessoais da Base PF/FGTS.
-- Mantido separado de CCI/CCA porque as bases têm critérios de recorte
-- distintos e sua soma produziria dupla contagem dos indicadores oficiais.

select
    {{ linha_financiada_data_iso('dt_assinatura') }} as data_contratacao,
    trim(faixa::text) as faixa_codigo,
    trim(tp_orcamento::text) as tipo_orcamento,
    trim(tpimovel::text) as tipo_imovel,
    {{ linha_financiada_numero('vlr_emprestimo') }} as valor_financiamento,
    current_timestamp as dt_silver
from {{ ref('bronze_geavo_fgts_pf') }}
where {{ linha_financiada_data_iso('dt_assinatura') }} is not null
