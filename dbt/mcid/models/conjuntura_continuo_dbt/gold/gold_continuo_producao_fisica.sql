{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Produção Física (PIM-PF) e Vendas no varejo de
-- material de construção (PMC) — variações dessazonalizadas. Página 3, seção 5.
-- Fonte: IBGE API (automatizado). PIM-PF: agregado 8886 (insumos típicos da
-- construção). PMC: agregado 8757, categoria 56734 (volume de vendas).
with
    pim as (
        select
            periodo,
            max(case when variavel_id = 11601 then valor end) as pim_pf_var_mes,
            max(case when variavel_id = 11603 then valor end) as pim_pf_var_acum_ano,
            max(case when variavel_id = 11604 then valor end) as pim_pf_var_12_meses
        from {{ ref("silver_continuo_ibge_pim_pf_brasil") }}
        group by periodo
    ),

    pmc as (
        select
            periodo,
            max(case when variavel_id = 11708 then valor end) as pmc_var_mes,
            max(case when variavel_id = 11710 then valor end) as pmc_var_acum_ano,
            max(case when variavel_id = 11711 then valor end) as pmc_var_12_meses
        from {{ ref("silver_continuo_ibge_pmc_construcao") }}
        where categoria_id = 56734
        group by periodo
    )

select
    coalesce(pim.periodo, pmc.periodo) as periodo,
    to_date(coalesce(pim.periodo, pmc.periodo), 'YYYYMM') as data_referencia,
    pim.pim_pf_var_mes,
    pim.pim_pf_var_acum_ano,
    pim.pim_pf_var_12_meses,
    pmc.pmc_var_mes,
    pmc.pmc_var_acum_ano,
    pmc.pmc_var_12_meses
from pim
full outer join pmc on pim.periodo = pmc.periodo
order by periodo desc
