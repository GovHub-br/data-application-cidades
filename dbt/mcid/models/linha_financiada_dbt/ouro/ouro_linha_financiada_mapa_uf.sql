{{ config(materialized='table') }}

-- Agregado territorial para o mapa do Brasil no Superset. O código ISO 3166-2
-- evita geocodificação ad-hoc e não depende das coordenadas ausentes nos
-- registros de empreendimento.
select
    'BR-' || upper(trim(uf)) as iso_3166_2,
    upper(trim(uf)) as uf,
    sum(quantidade_contratos) as quantidade_contratos,
    sum(valor_financiamento) as valor_financiamento,
    sum(quantidade_empreendimentos) as quantidade_empreendimentos,
    current_timestamp as dt_ouro
from {{ ref('ouro_linha_financiada_mapa_execucao') }}
where nullif(trim(uf), '') is not null
group by 1, 2
