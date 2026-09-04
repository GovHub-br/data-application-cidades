{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: SINAPI — custo médio m² (material + mão de
-- obra), variações. Página 6 (Preços). Fonte: IBGE (automatizado). Bate
-- exato com o boletim (DEZ/25: R$ 1.891,63 / +0,51% mês / +5,63% 12m).

with base as (
    select * from {{ ref('slv_ibge_sinapi') }}
)

select
    periodo,
    to_date(periodo, 'YYYYMM') as data_referencia,
    max(case when variavel_id = 48   then valor end) as custo_medio_m2,
    max(case when variavel_id = 1196 then valor end) as var_mes,
    max(case when variavel_id = 1197 then valor end) as var_acum_ano,
    max(case when variavel_id = 1198 then valor end) as var_12_meses
from base
group by periodo
order by periodo desc
