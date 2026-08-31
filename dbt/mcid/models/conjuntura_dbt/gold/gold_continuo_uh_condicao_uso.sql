{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: N° de UH por condição de uso — FGTS-PF e
-- SBPE Aquisição (novos x usados). Página 4.
--
-- Lado FGTS-PF é PARCIALMENTE automatizado (2026-08-29): sistema GEAVO da
-- Caixa, via silver_continuo_geavo_fgts_pf. Esta Silver contém o recorte
-- MCMV/faixas; ele não reproduz o total "Canal FGTS" publicado no boletim
-- (1T26), portanto estes campos não devem ser apresentados como total PF
-- até que a fonte complementar/definição seja incorporada.
--
-- Lado SBPE Aquisição (2026-08-30): o TOTAL passou a vir da ABECIP
-- automatizada (`silver_continuo_abecip_financiamentos`), que tem a série
-- mensal completa e corrente — a planilha manual do CEAG morria em set/2025.
-- Conferido contra o boletim 1T26: jan–mar/2026 = 77.867 UH, exatamente
-- 55.633 usadas + 22.234 novas do impresso; jan–mar/2025 = 89.338, idem.
--
-- A abertura novos × usados continua vindo da planilha manual: a fonte
-- automatizada publica só o total de aquisição, e inventar o rateio seria
-- pior que deixar nulo. Onde o manual não alcança, o total existe e o
-- split fica em branco.

with fgts_pf as (
    select
        ano, mes,
        sum(qtd_unidades) filter (where tipo_imovel = 'Novo')  as fgts_pf_uh_novos,
        sum(qtd_unidades) filter (where tipo_imovel = 'Usado') as fgts_pf_uh_usados,
        sum(valor_emprestimo) filter (where tipo_imovel = 'Novo')  as fgts_pf_milhoes_novos,
        sum(valor_emprestimo) filter (where tipo_imovel = 'Usado') as fgts_pf_milhoes_usados
    from {{ ref('silver_continuo_geavo_fgts_pf') }}
    group by ano, mes
),

sbpe_split as (
    select
        periodo, ano, mes,
        abecip_sbpe_fin_uh_aq_novos,
        abecip_sbpe_fin_uh_aq_usados,
        abecip_sbpe_fin_milhoes_aq_novos,
        abecip_sbpe_fin_milhoes_aq_usados
    from {{ ref('silver_continuo_manual_mensais') }}
    where coalesce(abecip_sbpe_fin_uh_aq_novos, abecip_sbpe_fin_uh_aq_usados) is not null
),

-- espinha da série: a fonte automatizada, que é a que está em dia
sbpe as (
    select
        a.ano::text || '-' || lpad(a.mes::text, 2, '0') as periodo,
        a.ano, a.mes,
        a.unidades_aquisicao       as abecip_sbpe_fin_uh_aq_total,
        a.valor_aquisicao_milhoes  as abecip_sbpe_fin_milhoes_aq_total,
        s.abecip_sbpe_fin_uh_aq_novos,
        s.abecip_sbpe_fin_uh_aq_usados,
        s.abecip_sbpe_fin_milhoes_aq_novos,
        s.abecip_sbpe_fin_milhoes_aq_usados
    from {{ ref('silver_continuo_abecip_financiamentos') }} a
    left join sbpe_split s on s.ano = a.ano and s.mes = a.mes
)

select
    coalesce(sbpe.periodo, fgts_pf.ano || '-' || lpad(fgts_pf.mes::text, 2, '0')) as periodo,
    coalesce(sbpe.ano, fgts_pf.ano)  as ano,
    coalesce(sbpe.mes, fgts_pf.mes)  as mes,
    make_date(coalesce(sbpe.ano, fgts_pf.ano)::int, coalesce(sbpe.mes, fgts_pf.mes)::int, 1) as data_referencia,
    fgts_pf.fgts_pf_uh_novos,
    fgts_pf.fgts_pf_uh_usados,
    fgts_pf.fgts_pf_milhoes_novos,
    fgts_pf.fgts_pf_milhoes_usados,
    sbpe.abecip_sbpe_fin_uh_aq_total,
    sbpe.abecip_sbpe_fin_milhoes_aq_total,
    sbpe.abecip_sbpe_fin_uh_aq_novos,
    sbpe.abecip_sbpe_fin_uh_aq_usados,
    sbpe.abecip_sbpe_fin_milhoes_aq_novos,
    sbpe.abecip_sbpe_fin_milhoes_aq_usados
from fgts_pf
full outer join sbpe on sbpe.ano = fgts_pf.ano and sbpe.mes = fgts_pf.mes
order by ano desc, mes desc
