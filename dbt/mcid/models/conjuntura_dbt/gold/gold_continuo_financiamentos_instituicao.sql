{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Novos Financiamentos Imobiliários SBPE por
-- instituição financeira, acumulado no ano. Página 3, seção 6.
--
-- **Tabela única da série de bancos.** Une as duas origens, com a
-- AUTOMATIZADA em primeiro lugar (decisão do Lucas, 2026-08-30):
--
--   1. ABECIP automatizado (`silver_continuo_abecip_instituicoes`) — traz o
--      BRB, Banrisul, Banpará, Safra, Poupex e Ailos em linha própria, o que
--      é mais detalhado que o próprio boletim, que agrupa tudo em "DEMAIS".
--   2. Planilha manual (CEAG), só onde a automatizada ainda não chegou.
--      Tem apenas cinco bancos nomeados + total; o BRB cai dentro de
--      "DEMAIS", que aqui é calculado por resíduo (total − nomeados).
--      Conferido contra o boletim 1T26: jan–mar/2025 dá 3.884, exatamente
--      BRB 1.949 + DEMAIS 1.935 do impresso.
--
-- A coluna `fonte` diz de onde veio cada linha. Quando a mesma competência
-- existir nas duas, a automatizada vence — é ela que tem a abertura completa.
--
-- Formato LONGO (uma linha por instituição): acompanha a forma da fonte, não
-- quebra quando entra ou sai banco, e é o que a tabela do boletim mostra.

with automatizado as (
    select
        data_referencia, ano, mes, competencia, instituicao,
        unidades_acumuladas_ano      as uh_acumulado_ano,
        volume_acumulado_ano_milhoes as volume_acumulado_ano_milhoes,
        unidades_mensais             as uh_mes,
        volume_mensal_milhoes        as volume_mes_milhoes,
        dt_ingest
    from {{ ref('silver_continuo_abecip_instituicoes') }}
    where modalidade = 'total_aquisicao_construcao'
),

-- competências já cobertas pela fonte automatizada; a manual não repete
cobertas as (select distinct ano, mes from automatizado),

manual_largo as (
    select
        ano, mes,
        abecip_sbpe_fin_uh_acum_caixa      as uh_caixa,
        abecip_sbpe_fin_uh_acum_itau       as uh_itau,
        abecip_sbpe_fin_uh_acum_bradesco   as uh_bradesco,
        abecip_sbpe_fin_uh_acum_santander  as uh_santander,
        abecip_sbpe_fin_uh_acum_bb         as uh_bb,
        abecip_sbpe_fin_uh_acum_total      as uh_total,
        abecip_sbpe_fin_milhoes_acum_caixa      as vl_caixa,
        abecip_sbpe_fin_milhoes_acum_itau       as vl_itau,
        abecip_sbpe_fin_milhoes_acum_bradesco   as vl_bradesco,
        abecip_sbpe_fin_milhoes_acum_santander  as vl_santander,
        abecip_sbpe_fin_milhoes_acum_bb         as vl_bb,
        abecip_sbpe_fin_milhoes_acum_total      as vl_total
    from {{ ref('silver_continuo_manual_mensais') }}
    where abecip_sbpe_fin_uh_acum_total is not null
      and not exists (
          select 1 from cobertas c
          where c.ano = {{ ref('silver_continuo_manual_mensais') }}.ano
            and c.mes = {{ ref('silver_continuo_manual_mensais') }}.mes
      )
),

manual as (
    select
        make_date(m.ano::int, m.mes::int, 1) as data_referencia,
        m.ano, m.mes,
        m.ano::text || '-' || lpad(m.mes::text, 2, '0') as competencia,
        x.instituicao,
        x.uh  as uh_acumulado_ano,
        x.vl  as volume_acumulado_ano_milhoes,
        null::numeric as uh_mes,
        null::numeric as volume_mes_milhoes,
        null::timestamp as dt_ingest
    from manual_largo m
    cross join lateral (
        select 'TOTAL' as instituicao, m.uh_total uh, m.vl_total vl
        union all select 'CAIXA', m.uh_caixa, m.vl_caixa
        union all select 'ITAU UNIBANCO', m.uh_itau, m.vl_itau
        union all select 'BRADESCO', m.uh_bradesco, m.vl_bradesco
        union all select 'SANTANDER', m.uh_santander, m.vl_santander
        union all select 'BANCO DO BRASIL', m.uh_bb, m.vl_bb
        -- resíduo: a planilha manual não discrimina BRB nem os menores
        union all select 'DEMAIS (inclui BRB)',
            m.uh_total - coalesce(m.uh_caixa,0) - coalesce(m.uh_itau,0)
                       - coalesce(m.uh_bradesco,0) - coalesce(m.uh_santander,0) - coalesce(m.uh_bb,0),
            m.vl_total - coalesce(m.vl_caixa,0) - coalesce(m.vl_itau,0)
                       - coalesce(m.vl_bradesco,0) - coalesce(m.vl_santander,0) - coalesce(m.vl_bb,0)
    ) x
),

unido as (
    select 'abecip_automatizado' as fonte, * from automatizado
    union all
    select 'planilha_manual' as fonte, * from manual
),

total as (
    select ano, mes,
           max(uh_acumulado_ano) filter (where instituicao = 'TOTAL') as uh_total,
           max(volume_acumulado_ano_milhoes) filter (where instituicao = 'TOTAL') as vl_total
    from unido group by ano, mes
)

select
    u.data_referencia, u.ano, u.mes, u.competencia, u.fonte,
    u.instituicao,
    u.uh_acumulado_ano,
    u.volume_acumulado_ano_milhoes,
    u.uh_mes,
    u.volume_mes_milhoes,
    case when t.uh_total > 0 then u.uh_acumulado_ano / t.uh_total end      as uh_participacao,
    case when t.vl_total > 0 then u.volume_acumulado_ano_milhoes / t.vl_total end as volume_participacao,
    u.dt_ingest
from unido u
left join total t on t.ano = u.ano and t.mes = u.mes
order by u.ano desc, u.mes desc, u.uh_acumulado_ano desc nulls last
