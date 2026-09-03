{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Índice ICST (confiança na construção, FGV-IBRE).
-- Página 7 (seção 8). Fonte automatizada (portal FGV Dados, com credencial) —
-- substituiu o dado manual em 2026-08-25. A FGV não publica as variações
-- prontas, então são calculadas aqui sobre o índice.
--
-- ⚠️ O BOLETIM USA A SÉRIE **COM AJUSTE SAZONAL**. Até 2026-08-30 este gold
-- expunha só a série original (sem ajuste), que não reproduz o publicado:
--   mar/2026 -> publicado 2,3%   · sem ajuste 1,73% · com ajuste **2,30%**
--   dez/2025 -> publicado -1,30% · sem ajuste -0,33% · com ajuste **-1,30%**
-- Duas batidas exatas confirmam qual série o boletim usa.
--
-- A descoberta demorou porque a seção do ICST no PDF sai ilegível: as camadas
-- de texto de DUAS edições ficam sobrepostas. Foi preciso separá-las à mão.
--
-- As duas séries ficam expostas: `*_com_ajuste` é a que reproduz o boletim e
-- deve alimentar o painel; `*_serie_original` fica como apoio.

with base as (
    select
        periodo,
        data_referencia,
        icst_com_ajuste_sazonal,
        icst_sem_ajuste_sazonal
    from {{ ref('slv_fgv_icst') }}
)

select
    periodo,
    extract(year from data_referencia)::int  as ano,
    extract(month from data_referencia)::int as mes,
    data_referencia,

    -- série com ajuste sazonal — a do boletim
    icst_com_ajuste_sazonal,
    (icst_com_ajuste_sazonal - lag(icst_com_ajuste_sazonal, 1) over (order by data_referencia))
        / nullif(lag(icst_com_ajuste_sazonal, 1) over (order by data_referencia), 0)
        as indice_icst_var_mes_com_ajuste,
    (icst_com_ajuste_sazonal - lag(icst_com_ajuste_sazonal, 12) over (order by data_referencia))
        / nullif(lag(icst_com_ajuste_sazonal, 12) over (order by data_referencia), 0)
        as indice_icst_var_mes_vs_mes_ano_ant_com_ajuste,

    -- série original — apoio
    icst_sem_ajuste_sazonal,
    (icst_sem_ajuste_sazonal - lag(icst_sem_ajuste_sazonal, 1) over (order by data_referencia))
        / nullif(lag(icst_sem_ajuste_sazonal, 1) over (order by data_referencia), 0)
        as indice_icst_var_mes_serie_original,
    (icst_sem_ajuste_sazonal - lag(icst_sem_ajuste_sazonal, 12) over (order by data_referencia))
        / nullif(lag(icst_sem_ajuste_sazonal, 12) over (order by data_referencia), 0)
        as indice_icst_var_mes_vs_mes_ano_ant_serie_original,
    (icst_sem_ajuste_sazonal - lag(icst_sem_ajuste_sazonal, extract(month from data_referencia)::int)
        over (order by data_referencia))
        / nullif(lag(icst_sem_ajuste_sazonal, extract(month from data_referencia)::int)
            over (order by data_referencia), 0)
        as indice_icst_var_acum_ano_serie_original
from base
order by data_referencia desc
