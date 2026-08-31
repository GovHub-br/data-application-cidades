{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Índice IMOB (IMOB.SA via Alpha Vantage) —
-- variações mensais. Página 7 (seção 8, Índices da Construção).
--
-- AUTOMATIZADO em 2026-08-27 (era 100% manual). Fórmulas validadas
-- EXATAS contra os 5 meses que já tínhamos manualmente salvos
-- (fev-jun/2026): fechamento do último pregão do mês;
-- var_mes = close / close(mês anterior) - 1;
-- var_mes_vs_mes_ano_ant = close / close(mesmo mês, ano anterior) - 1;
-- var_acum_ano = close / close(dezembro do ano anterior) - 1.
--
-- Corrigido também um bug na ingestão (ver comentário em
-- silver_continuo_infomoney_imob.sql): o parquet da lake só tinha ~100
-- pregões (Alpha Vantage em modo "compact", e o parquet era regravado do
-- zero a cada rodada com só o lote do dia); o Postgres
-- infomoney.acoes_imob acumula por upsert desde 2022-12-30 — parquet
-- reconstruído a partir de lá em 2026-08-27 (execução manual pontual;
-- a DAG diária (`infomoney_imob`) ainda regrava o parquet só com o lote
-- "compact" do dia — precisa ajustar pra regravar a partir do Postgres
-- acumulado, senão esse gold volta a ficar "curto" com o tempo).
--
-- Fonte única: a série automatizada. O coalesce com a planilha manual foi
-- removido em 2026-08-31 — a automatizada cobre 100% dos meses do gold, então
-- o ramo manual era código morto. Meio a meio esconde qual origem respondeu.
-- Removido também o comentário que descrevia o antigo arranjo pros
-- meses fora da cobertura do IMOB.SA (antes de 2022-12) ou onde a
-- automação não tiver dado por algum motivo.
--
-- ATENÇÃO: o mês corrente usa o último pregão disponível como proxy de
-- "fechamento do mês" (ainda não fechou) — os 3 valores desse mês mudam
-- a cada rodada até o mês virar. Mesmo comportamento que outros
-- indicadores contínuos já têm (ex.: CBIC), não é bug.

with fechamentos_mensais as (
    select
        date_trunc('month', data_pregao::date)::date as mes_ref,
        close::numeric as close_fechamento,
        row_number() over (
            partition by date_trunc('month', data_pregao::date)
            order by data_pregao::date desc
        ) as rn
    from {{ ref('silver_continuo_infomoney_imob') }}
    where close is not null
),

mensal as (
    select mes_ref, close_fechamento
    from fechamentos_mensais
    where rn = 1
),

dez_ano_anterior as (
    select
        extract(year from mes_ref)::int + 1 as ano_seguinte,
        close_fechamento as close_dez_ano_anterior
    from mensal
    where extract(month from mes_ref) = 12
),

automatico as (
    select
        extract(year from m.mes_ref)::int  as ano,
        extract(month from m.mes_ref)::int as mes,
        m.close_fechamento / lag(m.close_fechamento) over (order by m.mes_ref) - 1
            as indice_imob_var_mes,
        m.close_fechamento / lag(m.close_fechamento, 12) over (order by m.mes_ref) - 1
            as indice_imob_var_mes_vs_mes_ano_ant,
        m.close_fechamento / d.close_dez_ano_anterior - 1
            as indice_imob_var_acum_ano
    from mensal m
    left join dez_ano_anterior d on d.ano_seguinte = extract(year from m.mes_ref)::int
)

select
    a.ano || '-' || lpad(a.mes::text, 2, '0') as periodo,
    a.ano,
    a.mes,
    make_date(a.ano, a.mes, 1) as data_referencia,
    a.indice_imob_var_mes,
    a.indice_imob_var_mes_vs_mes_ano_ant,
    a.indice_imob_var_acum_ano
from automatico a
order by ano desc, mes desc
