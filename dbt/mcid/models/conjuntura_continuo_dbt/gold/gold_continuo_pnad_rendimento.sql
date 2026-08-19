{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: PNAD-C — rendimento médio real da construção x
-- total (R$), trimestre móvel. Página 3, seção 4. Fonte: IBGE via SIDRA
-- (automatizado). Obs.: série deflacionada — o IBGE re-baseia a cada trimestre.
with base as (select * from {{ ref("silver_continuo_ibge_pnad_construcao_rendimento") }})

select
    periodo,
    periodo_nome,
    to_date(periodo, 'YYYYMM') as data_referencia,
    max(case when categoria_id = '47949' then valor end) as rendimento_construcao_rs,
    max(case when categoria_id = '47946' then valor end) as rendimento_total_rs
from base
group by periodo, periodo_nome
order by periodo desc
