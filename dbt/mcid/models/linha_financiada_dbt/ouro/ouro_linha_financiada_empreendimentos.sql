{{ config(materialized='table') }}

with contratos as (
    select
        codigo_empreendimento,
        count(*) as quantidade_contratos,
        sum(coalesce(valor_financiamento, 0)) as valor_financiamento,
        max(ic_pro_moradia) as ic_pro_moradia,
        max(ic_mcmv_cidades) as ic_mcmv_cidades,
        max(ic_classe_media) as ic_classe_media
    from {{ ref('ouro_linha_financiada_base_agregada') }}
    where codigo_empreendimento is not null
    group by 1
)
select
    e.*,
    coalesce(c.quantidade_contratos, 0) as quantidade_contratos,
    coalesce(c.valor_financiamento, 0) as valor_financiamento,
    coalesce(c.ic_pro_moradia, false) as ic_pro_moradia,
    coalesce(c.ic_mcmv_cidades, false) as ic_mcmv_cidades,
    coalesce(c.ic_classe_media, false) as ic_classe_media,
    case
        when e.percentual_obra >= 100 then 'Concluído'
        when e.percentual_obra > 0 then 'Em execução'
        when e.percentual_obra = 0 then 'Não iniciado'
        else 'Sem posição de obra'
    end as situacao_execucao,
    current_timestamp as dt_gold
from {{ ref('prata_linha_financiada_empreendimento') }} e
left join contratos c using (codigo_empreendimento)
