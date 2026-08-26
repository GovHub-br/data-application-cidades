{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: PNAD-C — pessoas ocupadas na construção x total
-- (mil pessoas), trimestre móvel. Página 3, seção 4. Fonte: IBGE via SIDRA
-- (automatizado). Bate exato com o boletim.
with base as (select * from {{ ref("silver_continuo_ibge_pnad_construcao_ocupados") }})

select
    periodo,
    periodo_nome,
    to_date(periodo, 'YYYYMM') as data_referencia,
    max(case when categoria_id = '47949' then valor end) as ocupados_construcao_mil,
    max(case when categoria_id = '47946' then valor end) as ocupados_total_mil
from base
group by periodo, periodo_nome
order by periodo desc
