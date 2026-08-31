{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: PNAD-C — rendimento médio real da construção x
-- total (R$), trimestre móvel. Página 3, seção 4. Fonte: IBGE via SIDRA
-- (automatizado). Obs.: série deflacionada — o IBGE re-baseia a cada trimestre.

with base as (
    select * from {{ ref('silver_continuo_ibge_pnad_construcao_rendimento') }}
)

select
    periodo,
    -- Trimestre móvel: a API v3 devolve só o código do período (ex.: 202603),
    -- sem rótulo. Como o período é sempre o trimestre que TERMINA no mês
    -- indicado, o nome é derivável — não precisa vir da fonte.
    (
        (array['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'])[((right(periodo, 2)::int - 3 + 12) % 12) + 1] || '-' ||
        (array['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'])[((right(periodo, 2)::int - 2 + 12) % 12) + 1] || '-' ||
        (array['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'])[right(periodo, 2)::int] || ' ' || left(periodo, 4)
    )                                     as periodo_nome,
    to_date(periodo, 'YYYYMM') as data_referencia,
    max(case when categoria_id = '47949' then valor end) as rendimento_construcao_rs,
    max(case when categoria_id = '47946' then valor end) as rendimento_total_rs
from base
group by periodo
order by periodo desc
