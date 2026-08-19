{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Empregos — saldo e estoque na construção civil
-- x total da economia. Página 3, seção 4. Dado MANUAL (boletim.xlsx /
-- manual_conjuntura.dados_mensais). Obs.: silver_continuo_novo_caged (API,
-- recorte "construção de edifícios") fica como apoio/validação.

select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    emprego_const_saldo,
    emprego_const_saldo_var_mes,
    emprego_const_saldo_var_12_meses,
    emprego_const_estoque,
    emprego_const_estoque_var_mes,
    emprego_const_estoque_var_12_meses,
    caged_total_saldo,
    caged_total_saldo_var_mes,
    caged_total_saldo_var_12_meses,
    caged_total_estoque,
    caged_total_estoque_var_mes,
    caged_total_estoque_var_12_meses
from {{ ref('silver_continuo_manual_mensais') }}
where coalesce(emprego_const_saldo, caged_total_saldo) is not null
order by ano desc, mes desc
