{{ config(materialized="table") }}

-- Gold: Estágio ATUAL de cada obra do Pró-Moradia e como ela chegou lá. Réplica, para o
-- Pró-Moradia, da pergunta do Rural "que obras estão em andamento agora e em que estágio".
-- Grão: contrato.
--
-- Estágio = status_execucao_simplificado da ficha. Trajetória = avaliações mensais JÁ MEDIDAS
-- da execução de obra do Canal FGTS (o cronograma futuro fica de fora): quantas medições,
-- quanto avançou nos últimos 12 meses e desde quando o percentual não sobe.

with
    f as (select * from {{ ref("ouro_pro_moradia_ficha_contrato") }}),

    med as (
        select *,
               lag(percentual_obra_realizado) over (partition by cod_contrato order by dt_avaliacao) as pct_anterior,
               lag(situacao_obra) over (partition by cod_contrato order by dt_avaliacao) as situacao_anterior
        from {{ ref("prata_pro_moradia_execucao_obra") }}
        where not coalesce(ic_avaliacao_futura, false)
    ),

    traj as (
        select
            cod_contrato,
            count(*) as qt_medicoes,
            min(dt_avaliacao) as dt_primeira_medicao,
            max(dt_avaliacao) as dt_ultima_medicao,
            max(dt_avaliacao) filter (where percentual_obra_realizado > coalesce(pct_anterior, 0)) as dt_ultimo_avanco,
            count(*) filter (where situacao_anterior is not null and situacao_obra is distinct from situacao_anterior) as qt_mudancas_situacao,
            max(percentual_obra_realizado) - coalesce(
                max(percentual_obra_realizado) filter (where dt_avaliacao <= (select max(dt_avaliacao) from med) - interval '12 months'),
                0) as avanco_pp_12_meses
        from med
        group by cod_contrato
    )

select
    f.cod_contrato,
    f.contrato,
    f.contrato_municipio_empreendimento,
    f.tomador_codigo,
    f.tomador_nome,
    f.tomador_esfera,
    f.uf,
    f.regiao,
    f.municipio,
    f.tipo_intervencao,
    f.dt_assinatura,
    f.valor_contratado,
    f.vr_desembolsado,
    f.quantidade_uh,
    f.status_execucao_simplificado as estagio_obra,
    f.situacao_obra,
    f.percentual_execucao_fisica,
    f.percentual_obra_previsto,
    f.percentual_execucao_fisica - f.percentual_obra_previsto as desvio_cronograma_pp,
    f.percentual_execucao_financeira,
    f.ritmo_fisico_financeiro,
    f.dt_inicio_obra,
    f.dt_termino_obra,
    f.ic_paralisada,
    f.dias_sem_evolucao,
    f.motivo_paralisacao,
    t.qt_medicoes,
    t.dt_primeira_medicao,
    t.dt_ultima_medicao,
    t.dt_ultimo_avanco,
    (t.dt_ultima_medicao - t.dt_ultimo_avanco) / 30 as meses_sem_avanco,
    t.avanco_pp_12_meses,
    t.qt_mudancas_situacao,
    f.status_execucao_simplificado in ('Em Andamento', 'Paralisado', 'Não Iniciado') as ic_obra_em_aberto
from f
left join traj t on t.cod_contrato = f.cod_contrato
