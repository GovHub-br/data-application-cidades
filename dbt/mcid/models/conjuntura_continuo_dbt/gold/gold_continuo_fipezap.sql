{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Índice FipeZap de locação — número índice e
-- variações mensais. Página 7 (seção 8). Dado MANUAL (boletim.xlsx /
-- manual_conjuntura.dados_mensais). Obs.: silver_continuo_fipezap_locacao
-- (FIPE, automatizado) fica como apoio.

select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    indice_fipezap_numero_indice_locacao,
    indice_fipezap_locacao_var_mes,
    indice_fipezap_locacao_var_mes_vs_mes_ano_ant,
    indice_fipezap_locacao_acum_ano
from {{ ref('silver_continuo_manual_mensais') }}
where indice_fipezap_numero_indice_locacao is not null
order by ano desc, mes desc
