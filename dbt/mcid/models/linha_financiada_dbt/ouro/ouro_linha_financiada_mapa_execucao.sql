{{ config(materialized='table') }}

select
    codigo_municipio,
    municipio,
    uf,
    fonte_recurso,
    segmento_linha_financiada,
    faixa_codigo,
    programa,
    modalidade,
    tipo_imovel,
    ic_classe_media,
    ic_mcmv_cidades,
    ic_pro_moradia,
    count(*) as quantidade_contratos,
    count(distinct codigo_empreendimento) as quantidade_empreendimentos,
    sum(coalesce(valor_financiamento, 0)) as valor_financiamento,
    sum(coalesce(valor_desconto_fgts, 0) + coalesce(valor_desconto_ogu, 0)) as valor_descontos,
    sum(coalesce(valor_contrapartida_informada, 0)) as valor_contrapartida_informada,
    min(data_contratacao) as primeira_contratacao,
    max(data_contratacao) as ultima_contratacao,
    current_timestamp as dt_gold
from {{ ref('ouro_linha_financiada_base_agregada') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
