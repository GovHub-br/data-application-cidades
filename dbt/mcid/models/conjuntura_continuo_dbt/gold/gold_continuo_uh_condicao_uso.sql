{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: N° de UH por condição de uso — SBPE Aquisição
-- (novos x usados). Página 4. Dado MANUAL (boletim.xlsx /
-- manual_conjuntura.dados_mensais). Obs.: recorte FGTS-PF por condição de uso
-- não está disponível na planilha oficial — apenas o lado SBPE.

select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    abecip_sbpe_fin_uh_aq_novos,
    abecip_sbpe_fin_uh_aq_usados,
    abecip_sbpe_fin_milhoes_aq_novos,
    abecip_sbpe_fin_milhoes_aq_usados
from {{ ref('silver_continuo_manual_mensais') }}
where coalesce(abecip_sbpe_fin_uh_aq_novos, abecip_sbpe_fin_uh_aq_usados) is not null
order by ano desc, mes desc
