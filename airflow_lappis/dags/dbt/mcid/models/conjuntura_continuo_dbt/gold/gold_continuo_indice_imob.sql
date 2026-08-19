{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Índice IMOB — variações mensais. Página 7
-- (seção 8, Índices da Construção). Dado MANUAL (boletim.xlsx /
-- manual_conjuntura.dados_mensais). Obs.: silver_continuo_infomoney_imob
-- (Alpha Vantage, preço de fechamento IMOB.SA) fica como apoio/nível bruto.

select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    indice_imob_var_mes,
    indice_imob_var_mes_vs_mes_ano_ant,
    indice_imob_var_acum_ano
from {{ ref('silver_continuo_manual_mensais') }}
where indice_imob_var_mes is not null
order by ano desc, mes desc
