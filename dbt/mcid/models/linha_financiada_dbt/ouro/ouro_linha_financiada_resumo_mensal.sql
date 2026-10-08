{{ config(materialized='table') }}

select
    competencia_contratacao,
    ano_contratacao,
    mes_contratacao,
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
    count(distinct codigo_empreendimento) as quantidade_empreendimentos,
    sum(coalesce(valor_financiamento, 0)) as valor_financiamento,
    sum(coalesce(valor_desconto_fgts, 0)) as valor_desconto_fgts,
    sum(coalesce(valor_desconto_ogu, 0)) as valor_desconto_ogu,
    sum(coalesce(valor_contrapartida_informada, 0)) as valor_contrapartida_informada,
    sum(coalesce(valor_total_recursos_identificados, 0)) as valor_total_recursos_identificados,
    avg(taxa_juros_inicial) as taxa_juros_media,
    avg(prazo_meses) as prazo_medio_meses,
    current_timestamp as dt_gold
from {{ ref('ouro_linha_financiada_base_agregada') }}
where competencia_contratacao is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
