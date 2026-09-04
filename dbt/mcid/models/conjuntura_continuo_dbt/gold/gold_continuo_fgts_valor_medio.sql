{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: FGTS — Valor médio dos imóveis financiados
-- (total e Faixa 1), variações. Página 6/7 (Preços). Dado MANUAL (validado
-- vs boletim 4T25: DEZ/25 = R$ 245.959, +2,89% mês, +12,14% em 12m).
select
    ano,
    mes,
    to_date(mes, 'MM/YYYY') as data_referencia,
    valor_medio_fgts,
    var_fgts_mes,
    var_fgts_12m,
    var_fgts_acum_ano,
    valor_medio_fgts_f1,
    var_fgts_f1_mes,
    var_fgts_f1_12m,
    var_fgts_f1_acum_ano
from {{ ref("silver_continuo_fgts_valor_medio_imoveis") }}
order by to_date(mes, 'MM/YYYY') desc
