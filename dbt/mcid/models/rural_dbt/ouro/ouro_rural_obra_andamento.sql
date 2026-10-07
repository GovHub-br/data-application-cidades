{{ config(materialized="table") }}

-- Gold: Estágio ATUAL de cada obra do Novo Rural e como ela chegou lá. Responde "que entidades
-- têm obra em andamento neste momento, e em que estágio?".
-- Grão: APF presente na série mensal de obra (MONIT_MOV_OBRA_RURAL_MENSAL).
--
-- Estágio = situação da operação decodificada pelo layout (seed dominio_rural_obra). A série
-- começa em dez/2025, então "primeiro estágio" é o primeiro retrato disponível, não o início da
-- obra. A situação GEHIS (Normal / Em atenção / Risco de paralisação...) é a avaliação da
-- CAIXA no mesmo período e vem do arquivo GEHIS mais recente para a APF.

with
    serie as (select * from {{ ref("prata_rural_obra_mensal_serie") }}),

    atual as (
        select distinct on (apf) * from serie order by apf, dt_referencia desc
    ),

    trajetoria as (
        select
            apf,
            count(*) as qt_meses_na_serie,
            min(dt_referencia) as dt_primeiro_retrato,
            (array_agg(estagio_obra order by dt_referencia))[1] as estagio_primeiro_retrato,
            (array_agg(percentual_obra_realizada order by dt_referencia))[1] as percentual_primeiro_retrato,
            count(*) filter (where estagio_mes_anterior is not null and estagio_obra <> estagio_mes_anterior) as qt_mudancas_estagio,
            max(dt_referencia) filter (where estagio_mes_anterior is not null and estagio_obra <> estagio_mes_anterior) as dt_ultima_mudanca_estagio,
            count(*) filter (where estagio_obra = 'Paralisada') as qt_meses_paralisada
        from serie
        group by apf
    ),

    gehis as (
        select distinct on (apf) apf, situacao_obra as situacao_gehis, dt_referencia as dt_referencia_gehis,
               dt_prevista_conclusao as dt_prevista_conclusao_gehis
        from {{ ref("prata_rural_gehis_andamento_obra") }}
        order by apf, dt_referencia desc
    ),

    op as (select * from {{ ref("prata_rural_operacao_eo") }})

select
    a.apf,
    o.empreendimento_nome,
    o.municipio,
    o.uf,
    o.regiao,
    o.eo_cnpj,
    o.eo_nome,
    o.fonte_eo,
    o.dt_contratacao,
    o.quantidade_uh_contratadas,
    o.valor_contratado,
    o.valor_desembolsado,
    a.dt_referencia as dt_referencia_atual,
    a.estagio_obra,
    a.situacao_operacao,
    a.andamento_operacao,
    a.percentual_obra_prevista,
    a.percentual_obra_realizada,
    a.desvio_cronograma_pp,
    a.dt_paralisacao,
    a.motivo_paralisacao,
    a.motivo_nao_retomada,
    a.motivo_distrato,
    a.detalhe_paralisacao,
    a.dt_previsao_entrega,
    t.qt_meses_na_serie,
    t.dt_primeiro_retrato,
    t.estagio_primeiro_retrato,
    a.percentual_obra_realizada - t.percentual_primeiro_retrato as avanco_obra_pp_na_serie,
    t.qt_mudancas_estagio,
    coalesce(t.dt_ultima_mudanca_estagio, t.dt_primeiro_retrato) as dt_desde_estagio_atual,
    t.qt_meses_paralisada,
    g.situacao_gehis,
    g.dt_referencia_gehis,
    g.dt_prevista_conclusao_gehis,
    a.estagio_obra in ('Em obra', 'Em obra - no prazo', 'Em obra - atrasada', 'Paralisada', 'Não iniciada') as ic_obra_em_aberto
from atual a
join trajetoria t on t.apf = a.apf
left join op o on o.apf = a.apf
left join gehis g on g.apf = a.apf
