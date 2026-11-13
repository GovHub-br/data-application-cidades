{{ config(materialized="table") }}

-- Gold: evolução do EMPENHO do FNHIS Sub-50 por mês do retrato do TransfereGov, já agregada. Grão: mês × UF.
-- Termos ativos (sem anulados/rescindidos) com assinatura. % empenhado = Σ empenhado acumulado ÷ Σ repasse
-- previsto (recalcule a partir das somas; não some pct_empenhado). Não é uma Curva S de obra: o Sub-50 não tem
-- % físico nas bases e o empenho é liberado em parcelas fixas (≈10%, 20%, 30%, 50%…), por isso a linha anda em
-- degraus. ic_retrato_completo marca os meses em que o retrato tem pelo menos 80% do maior número de termos já
-- visto (proteção contra um retrato que venha cortado; hoje todos estão completos). Lê só prata.

with
    base as (select * from ({{ fnhis_curva_s_termo_mes() }}) b),
    t as (select numero_proposta_transferegov, uf from {{ ref("prata_fnhis_termo_compromisso") }}),
    por_uf as (
        select
            b.mes,
            t.uf,
            count(*) as qt_termos,
            count(*) filter (where b.pct_financeiro > 0) as qt_termos_com_empenho,
            sum(b.peso) as valor_repasse,
            sum(b.pct_financeiro * b.peso / 100.0) as valor_empenhado_acumulado
        from base b
        join t on t.numero_proposta_transferegov = b.chave
        group by b.mes, t.uf
    ),
    por_mes as (select mes, sum(qt_termos) as qt_termos_mes from por_uf group by mes)

select
    md5(concat_ws('|', u.mes::text, u.uf)) as id_curva_s,
    u.mes,
    (u.mes = max(u.mes) over ()) as ic_ultimo_mes,
    (pm.qt_termos_mes >= 0.8 * max(pm.qt_termos_mes) over ()) as ic_retrato_completo,
    u.uf,
    '1.310/2024'::text as portaria_selecao,
    u.qt_termos,
    u.qt_termos_com_empenho,
    u.valor_repasse,
    u.valor_empenhado_acumulado,
    round(100.0 * u.valor_empenhado_acumulado / nullif(u.valor_repasse, 0), 2) as pct_empenhado
from por_uf u
join por_mes pm on pm.mes = u.mes
