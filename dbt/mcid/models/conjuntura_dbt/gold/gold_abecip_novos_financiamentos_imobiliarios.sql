{{ config(materialized="table") }}

with
    base as (
        select ano, mes, banco, valor_bi, uh, dt_ingest, dt_silver
        from {{ ref("silver_abecip_novos_financiamentos_imobiliarios") }}
    ),

    total as (
        select ano, mes, sum(valor_bi) as total_bi, sum(uh) as total_uh
        from base
        where banco != 'TOTAL'
        group by ano, mes
    ),

    com_perc as (
        select
            b.ano,
            b.mes,
            b.banco,
            round(b.valor_bi::numeric, 1) as valor_bi,
            round((b.valor_bi / nullif(t.total_bi, 0)) * 100, 1) as perc_valor,
            b.uh,
            round((b.uh::numeric / nullif(t.total_uh, 0)) * 100, 1) as perc_uh,
            case
                b.banco
                when 'TOTAL'
                then 0
                when 'CEF (CAIXA)'
                then 1
                when 'ITAU'
                then 2
                when 'BRADESCO'
                then 3
                when 'SANTANDER'
                then 4
                when 'BRB'
                then 5
                when 'BB (BANCO DO BRASIL)'
                then 6
                when 'DEMAIS'
                then 7
            end as ordem,
            b.dt_ingest,
            b.dt_silver
        from base b
        left join total t on b.ano = t.ano and b.mes = t.mes
    ),

    ano_anterior as (
        select ano, mes, banco, valor_bi as valor_bi_ant, uh as uh_ant from com_perc
    )

select
    c.ano,
    c.mes,
    c.banco,
    c.valor_bi,
    c.perc_valor as perc_valor,
    round(((c.valor_bi / nullif(a.valor_bi_ant, 0)) - 1) * 100, 0) as var_ano_valor,
    c.uh,
    c.perc_uh,
    round(((c.uh::numeric / nullif(a.uh_ant, 0)) - 1) * 100, 0) as var_ano_uh,
    c.ordem,
    {{ add_metadata_timestamps("gold") }}
from com_perc c
left join ano_anterior a on c.banco = a.banco and a.ano = c.ano - 1 and a.mes = c.mes
order by c.ano, c.mes, c.ordem
