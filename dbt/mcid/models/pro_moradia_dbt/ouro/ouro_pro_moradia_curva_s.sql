{{ config(materialized="table") }}

-- Gold: Curva S físico-financeira do Pró-Moradia por mês do calendário, já agregada. Grão: mês × UF × tipo de
-- intervenção × situação na carteira. Carteira: contratos assinados até o mês que já têm alguma medição.
-- situacao_carteira (no mês): 'Concluída' (físico carregado = 100%), 'Sem medição há 12+ meses' (última medição
-- real 12 meses ou mais antes do mês: provável paralisação ou contrato que parou de reportar) e 'Em execução'.
-- Para a curva da carteira ativa filtre situacao_carteira = 'Em execução'; sem o filtro a curva é o estoque de
-- 30 anos de contratos (os concluídos ficam a 100% para sempre). O % previsto vem quase sempre 0 no Canal FGTS
-- (474 de 653 avaliações desde 2024): não use como cronograma. Físico: medição do mês ou, sem
-- medição, a última anterior (antes da primeira medição o contrato não entra no físico). Financeiro: desembolso
-- acumulado ÷ valor contratado, só dos contratos com desembolso no Canal FGTS. Pesos por VALOR CONTRATADO (há
-- contratos sem UH). Some as colunas de soma e recalcule os percentuais; não some as colunas pct_*. Lê só prata.

with
    base as (select * from ({{ pro_moradia_curva_s_obra_mes() }}) b),
    c as (select * from {{ ref("prata_pro_moradia_contrato") }} where dt_assinatura is not null),
    limites as (select min(mes) as ini, max(mes) as fim from base),
    meses as (select generate_series(ini, fim, interval '1 month')::date as mes from limites),
    primeiro as (select chave, min(mes) as mes_ini from base group by chave),
    grade as (
        select p.chave, m.mes, b.pct_fisico_realizado, b.pct_fisico_previsto, b.pct_financeiro,
               max(case when b.pct_fisico_realizado is not null then m.mes end)
                   over (partition by p.chave order by m.mes) as mes_ultima_medicao
        from primeiro p
        join meses m on m.mes >= p.mes_ini
        left join base b on b.chave = p.chave and b.mes = m.mes
    ),
    grupos as (
        select g.*,
               count(g.pct_fisico_realizado) over w as g_r,
               count(g.pct_fisico_previsto) over w as g_p,
               count(g.pct_financeiro) over w as g_f
        from grade g
        window w as (partition by g.chave order by g.mes)
    ),
    carregado as (
        select chave, mes, mes_ultima_medicao,
               max(pct_fisico_realizado) over (partition by chave, g_r) as pct_realizado,
               max(pct_fisico_previsto) over (partition by chave, g_p) as pct_previsto,
               case when g_f > 0 then max(pct_financeiro) over (partition by chave, g_f) end as pct_financeiro
        from grupos
    ),
    classificado as (
        select k.*,
               case
                   when k.pct_realizado >= 100 then 'Concluída'
                   when k.mes_ultima_medicao <= (k.mes - interval '12 months')::date then 'Sem medição há 12+ meses'
                   else 'Em execução'
               end as situacao_carteira
        from carregado k
    )

select
    md5(concat_ws('|', k.mes::text, c.uf, coalesce(c.tipo_intervencao, 'Não informado'), k.situacao_carteira)) as id_curva_s,
    k.mes,
    (k.mes = max(k.mes) over ()) as ic_ultimo_mes,
    c.uf,
    coalesce(c.tipo_intervencao, 'Não informado') as tipo_intervencao,
    k.situacao_carteira,
    count(*) as qt_contratos,
    sum(c.valor_contratado) as valor_contratado,
    sum(c.valor_contratado) filter (where k.pct_realizado is not null) as valor_com_medicao,
    sum(k.pct_realizado * c.valor_contratado) filter (where k.pct_realizado is not null) as fis_realizado_x_valor,
    sum(c.valor_contratado) filter (where k.pct_previsto is not null) as valor_com_previsao,
    sum(k.pct_previsto * c.valor_contratado) filter (where k.pct_previsto is not null) as fis_previsto_x_valor,
    sum(c.valor_contratado) filter (where k.pct_financeiro is not null) as valor_com_desembolso,
    sum(k.pct_financeiro * c.valor_contratado / 100.0) filter (where k.pct_financeiro is not null) as vr_desembolsado_acumulado,
    round(sum(k.pct_realizado * c.valor_contratado) filter (where k.pct_realizado is not null)
          / nullif(sum(c.valor_contratado) filter (where k.pct_realizado is not null), 0), 2) as pct_fisico_realizado,
    round(sum(k.pct_previsto * c.valor_contratado) filter (where k.pct_previsto is not null)
          / nullif(sum(c.valor_contratado) filter (where k.pct_previsto is not null), 0), 2) as pct_fisico_previsto,
    round(sum(k.pct_financeiro * c.valor_contratado) filter (where k.pct_financeiro is not null)
          / nullif(sum(c.valor_contratado) filter (where k.pct_financeiro is not null), 0), 2) as pct_financeiro
from classificado k
join c on c.cod_contrato = k.chave
group by k.mes, c.uf, coalesce(c.tipo_intervencao, 'Não informado'), k.situacao_carteira
