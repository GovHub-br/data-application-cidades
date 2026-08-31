{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Ticket Médio de Lançamentos (MRV, Direcional,
-- Tenda, Cury) vs INCC — número índice e variações trimestrais, base 4T2020.
-- Página 6/7 (Preços).
--
-- Lado INCC agora é AUTOMATIZADO (FGV-IBRE, via gold_continuo_incc_m) —
-- pega o índice do último mês de cada trimestre, igual o boletim já fazia
-- na planilha manual (conferido: o valor antigo em
-- manual_conjuntura.dados_trimestrais.precos_incc_tri batia com o índice
-- de fechamento do trimestre, não com uma versão rebasada).
--
-- Lado "ticket médio de lançamentos" por construtora continua MANUAL
-- (silver_continuo_manual_trimestrais / manual_conjuntura.dados_trimestrais)
-- — precisaria do VGV (valor geral de lançamentos) de cada construtora por
-- trimestre pra calcular sozinho (ticket médio = VGV / unidades), e hoje só
-- temos as unidades no nosso balanço das empresas, não o VGV. Ainda não é
-- 100% automático, diferente do que tinha sido combinado — ver
-- docs/conjuntura-fontes-dbt.md.

with incc_mensal as (
    select
        mes,
        indice,
        extract(year from mes)::int  as ano,
        extract(month from mes)::int as mes_num
    from {{ ref('gold_continuo_incc_m') }}
),

incc_trimestral as (
    select
        ano,
        case mes_num
            when 3  then 1
            when 6  then 2
            when 9  then 3
            when 12 then 4
        end as trimestre,
        indice as precos_incc_tri
    from incc_mensal
    where mes_num in (3, 6, 9, 12)
),

incc_base_4t2020 as (
    select precos_incc_tri as indice_base
    from incc_trimestral
    where ano = 2020 and trimestre = 4
),

incc as (
    select
        atual.ano,
        atual.trimestre,
        atual.precos_incc_tri,
        (atual.precos_incc_tri - anterior.precos_incc_tri) / nullif(anterior.precos_incc_tri, 0)
            as incc_var_tri_ant,
        (atual.precos_incc_tri - base.indice_base) / nullif(base.indice_base, 0)
            as incc_var_acum_4t2020
    from incc_trimestral atual
    left join incc_trimestral anterior
        on anterior.ano * 4 + anterior.trimestre = atual.ano * 4 + atual.trimestre - 1
    cross join incc_base_4t2020 base
)

select
    m.periodo,
    m.data_referencia,
    m.edicao,
    m.ano,
    m.trimestre,
    incc.precos_incc_tri,
    incc.incc_var_tri_ant,
    incc.incc_var_acum_4t2020,
    m.ticket_medio_lancamentos_mrv,
    m.ticket_medio_lancamentos_mrv_var_tri_ant,
    m.ticket_medio_lancamentos_mrv_var_acum_4t2020,
    m.ticket_medio_lancamentos_direcional,
    m.ticket_medio_lancamentos_direcional_var_tri_ant,
    m.ticket_medio_lancamentos_direcional_var_acum_4t2020,
    m.ticket_medio_lancamentos_tenda,
    m.ticket_medio_lancamentos_tenda_var_tri_ant,
    m.ticket_medio_lancamentos_tenda_var_acum_4t2020,
    m.ticket_medio_lancamentos_cury,
    m.ticket_medio_lancamentos_cury_var_tri_ant,
    m.ticket_medio_lancamentos_cury_var_acum_4t2020
from {{ ref('silver_continuo_manual_trimestrais') }} m
left join incc
    on incc.ano = m.ano
    and incc.trimestre = m.trimestre
order by m.ano desc, m.trimestre desc
