{{ config(materialized="table") }}

-- Gold: situação das parcelas de empenho de cada termo de compromisso do FNHIS Sub-50. Grão: termo (número da
-- proposta no TransfereGov), só termos ativos (sem anulados/rescindidos) com assinatura. Lê só prata.
-- Substitui a "curva fora da curva" do Sub-50: o empenho sai em parcelas fixas, então comparar com quartis por
-- idade não diz nada (quase todos ficam em ~10%). O que interessa é QUANTAS parcelas o termo já teve e HÁ QUANTO
-- TEMPO não recebe uma nova.
--   qt_parcelas_empenhadas   nº de vezes que o empenho acumulado subiu nos retratos (+1 se o 1º retrato já veio
--                            com empenho). Os retratos começam em mar/2025: o que foi empenhado antes conta como 1.
--   dt_ultimo_empenho        1º dia do mês do retrato em que o empenho subiu pela última vez (ou do 1º retrato, se
--                            nunca subiu depois dele — ic_ultimo_empenho_antes_dos_retratos = true).
--   meses_sem_novo_empenho   meses entre dt_ultimo_empenho e o retrato mais recente da base.
--   situacao_parcelas        'Sem empenho' | 'Sem novo empenho há 12+ meses' | 'Sem novo empenho há 6 a 11 meses'
--                            | 'Empenho nos últimos 6 meses'.
--   faixa_empenho            % empenhado do repasse em faixas ordenáveis pelo prefixo.

with
    base as (select * from ({{ fnhis_curva_s_termo_mes() }}) b),
    t as (select * from {{ ref("prata_fnhis_termo_compromisso") }}),
    corte as (select max(mes) as dt_referencia_base from base),
    serie as (
        select
            b.*,
            coalesce(b.pct_financeiro, 0) * b.peso / 100.0 as vr_empenhado,
            lag(coalesce(b.pct_financeiro, 0) * b.peso / 100.0) over (partition by b.chave order by b.mes) as vr_anterior,
            row_number() over (partition by b.chave order by b.mes) as n_retrato
        from base b
    ),
    eventos as (
        select
            chave,
            count(*) filter (where (n_retrato = 1 and vr_empenhado > 0.01)
                                or (n_retrato > 1 and vr_empenhado - vr_anterior > 0.01)) as qt_parcelas_empenhadas,
            max(mes) filter (where (n_retrato = 1 and vr_empenhado > 0.01)
                                or (n_retrato > 1 and vr_empenhado - vr_anterior > 0.01)) as dt_ultimo_empenho,
            min(mes) as dt_primeiro_retrato,
            count(*) as qt_retratos
        from serie
        group by chave
    ),
    ultimo as (select distinct on (chave) * from base order by chave, mes desc)

select
    u.chave as numero_proposta_transferegov,
    t.numero_proposta,
    t.proponente_nome,
    t.uf,
    t.municipio,
    t.regiao,
    t.dt_assinatura,
    extract(year from t.dt_assinatura)::int as ano_assinatura,
    t.objeto,
    u.situacao as situacao_instrumento,
    u.idade_meses,
    u.peso as valor_repasse,
    round(coalesce(u.pct_financeiro, 0) * u.peso / 100.0, 2) as valor_empenhado_acumulado,
    coalesce(u.pct_financeiro, 0) as pct_empenhado,
    case
        when coalesce(u.pct_financeiro, 0) <= 0 then '0. Sem empenho'
        when u.pct_financeiro <= 10.5 then '1. Até 10%'
        when u.pct_financeiro <= 20.5 then '2. 10% a 20%'
        when u.pct_financeiro <= 30.5 then '3. 20% a 30%'
        when u.pct_financeiro <= 50.5 then '4. 30% a 50%'
        when u.pct_financeiro < 99.5 then '5. 50% a 99%'
        else '6. 100%'
    end as faixa_empenho,
    e.qt_parcelas_empenhadas,
    e.qt_retratos,
    e.dt_primeiro_retrato,
    u.mes as dt_ultimo_retrato,
    e.dt_ultimo_empenho,
    (e.dt_ultimo_empenho = e.dt_primeiro_retrato) as ic_ultimo_empenho_antes_dos_retratos,
    k.dt_referencia_base,
    case when e.dt_ultimo_empenho is not null then
        (extract(year from k.dt_referencia_base)::int - extract(year from e.dt_ultimo_empenho)::int) * 12
        + extract(month from k.dt_referencia_base)::int - extract(month from e.dt_ultimo_empenho)::int
    end as meses_sem_novo_empenho,
    case
        when e.dt_ultimo_empenho is null or coalesce(u.pct_financeiro, 0) <= 0 then 'Sem empenho'
        when e.dt_ultimo_empenho <= (k.dt_referencia_base - interval '12 months')::date then 'Sem novo empenho há 12+ meses'
        when e.dt_ultimo_empenho <= (k.dt_referencia_base - interval '6 months')::date then 'Sem novo empenho há 6 a 11 meses'
        else 'Empenho nos últimos 6 meses'
    end as situacao_parcelas,
    '1.310/2024'::text as portaria_selecao
from ultimo u
join eventos e on e.chave = u.chave
join t on t.numero_proposta_transferegov = u.chave
cross join corte k
