{{ config(materialized="table") }}

-- Prata: Última posição MEDIDA de cada contrato, com o diagnóstico de prazo.
-- Fonte: prata_financiado_obra_mensal + prata_financiado_contrato_pj + paralisadas
-- Grão: uma linha por `cod_contrato`.
--
-- É a tabela que responde "quais projetos estão fora do prazo ou tendem a atrasar".
-- Três cuidados sustentam o número:
--
--   1. só entra linha medida (`ic_medido`), senão o cronograma futuro vira atraso;
--   2. `dt_ultima_medicao` fica exposta porque a maior parte da carteira é antiga e
--      já concluída. Ler "atrasado" sem olhar a idade da medição é ler o passado
--      como se fosse hoje;
--   3. obra concluída (`6`, `8`, `E`) não é atraso, mesmo com desvio positivo.
with
    medido as (
        select *
        from {{ ref("prata_financiado_obra_mensal") }}
        where ic_medido and dt_competencia is not null
    ),

    ultima as (
        select cod_contrato, max(dt_competencia) as dt_competencia
        from medido
        group by cod_contrato
    ),

    posicao as (
        select m.*
        from medido as m
        inner join
            ultima as u
            on m.cod_contrato = u.cod_contrato
            and m.dt_competencia = u.dt_competencia
    ),

    -- Um contrato pode ter mais de uma linha na mesma competência; fica a de maior
    -- execução realizada, que é a medição mais avançada do mês.
    posicao_unica as (
        select distinct
            on (cod_contrato) *
        from posicao
        order by cod_contrato asc, pct_realizado desc nulls last
    ),

    paralisada as (
        select
            cod_contrato,
            dias_sem_evolucao,
            faixa_paralisacao,
            dt_previsao_conclusao_objeto
        from {{ ref("prata_financiado_paralisacao") }}
    )

select
    p.cod_contrato,
    c.apf,
    c.cod_empreendimento,
    c.cod_linha,
    c.linha,
    c.ic_apoio_producao,
    c.tipo_operacao,
    c.tomador_nome,
    c.uf,
    c.vr_contratado,
    c.dt_assinatura,

    p.dt_competencia as dt_ultima_medicao,
    p.cod_situacao_obra,
    p.situacao_obra,
    p.pct_previsto,
    p.pct_realizado,
    p.pct_desvio,
    p.dt_ultima_vistoria,
    p.providencias,

    -- Meses entre a última medição e a remessa. Quanto maior, menos o diagnóstico
    -- fala do presente.
    (
        date_part('year', age(p.criado_em::date, p.dt_competencia)) * 12
        + date_part('month', age(p.criado_em::date, p.dt_competencia))
    )::int as meses_desde_medicao,

    -- Situação de obra declarada pela própria CAIXA.
    p.cod_situacao_obra = '3' as ic_atrasada,
    p.cod_situacao_obra = '4' as ic_paralisada,
    p.cod_situacao_obra in ('6', '8', 'E') as ic_concluida,

    -- Atraso MEDIDO, que é coisa diferente da situação declarada: o desvio entre o
    -- previsto e o realizado acumulados. Obra concluída fica de fora.
    case
        when p.cod_situacao_obra in ('6', '8', 'E')
        then false
        when p.pct_previsto is null or p.pct_realizado is null
        then null
        else p.pct_previsto - p.pct_realizado > 10
    end as ic_desvio_acima_10pp,

    par.cod_contrato is not null as ic_no_relatorio_de_paralisadas,
    par.dias_sem_evolucao,
    par.faixa_paralisacao,
    par.dt_previsao_conclusao_objeto,

    p.arquivo_de_origem,
    p.criado_em
from posicao_unica as p
left join {{ ref("prata_financiado_contrato_pj") }} as c on p.cod_contrato = c.cod_contrato
left join paralisada as par on p.cod_contrato = par.cod_contrato
