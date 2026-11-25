{{ config(materialized="table") }}

-- Ouro: indicação de demanda e dossiês dos empreendimentos FAR da lista "sem indicação de demanda",
-- na remessa mais recente, com a variação desde a remessa anterior e os dois alertas pedidos pela
-- equipe do MCID:
--   alerta_50_sem_demanda  obra a partir de 50% e nenhuma família indicada (zero pesquisas no SIGDH);
--   alerta_60_sem_dossie   obra a partir de 60% e nenhum dossiê enviado.
-- Os limites de 50% e 60% são regra de acompanhamento informada pela equipe; a norma de origem ainda
-- não foi registrada no repositório.
-- Grão: APF. Chave: apf.
-- Fontes: prata_ts_demanda_dossie (remessas de 02/09 e 06/10/2026), prata_ts_acompanhamento (UF,
-- município, UH e situação do TS quando a remessa não traz) e prata_ts_empreendimento_perfil (mapa).

with
    ordenada as (
        select d.*, row_number() over (partition by apf order by dt_referencia desc) as ordem
        from {{ ref("prata_ts_demanda_dossie") }} d
    ),

    atual as (select * from ordenada where ordem = 1),
    anterior as (select * from ordenada where ordem = 2),

    cadastro as (
        select distinct on (apf) apf, qt_uh, dt_contratacao, municipio, uf, ente_cnpj, ente_nome, construtora_nome
        from {{ ref("prata_ts_demanda_dossie") }}
        where uf is not null
        order by apf, dt_referencia desc
    ),

    ts as (
        select distinct on (apf) apf, id_ts, municipio, uf, qt_uh, situacao_ts, dt_contratacao
        from {{ ref("prata_ts_acompanhamento") }}
        where linha = 'FAR'
        order by apf, agente_financeiro
    ),

    perfil as (
        select distinct on (apf) apf, latitude, longitude, tipo_edificacao, regime, publico_tradicional_estimado
        from {{ ref("prata_ts_empreendimento_perfil") }}
        where linha = 'FAR'
        order by apf, agente_financeiro
    ),

    junto as (
        select
            at.apf,
            ts.id_ts,
            at.empreendimento_nome,
            coalesce(cad.municipio, ts.municipio) as municipio,
            coalesce(cad.uf, ts.uf) as uf,
            coalesce(cad.qt_uh, ts.qt_uh) as qt_uh,
            coalesce(cad.dt_contratacao, ts.dt_contratacao) as dt_contratacao,
            cad.ente_cnpj,
            cad.ente_nome,
            cad.construtora_nome,
            pf.latitude,
            pf.longitude,
            pf.tipo_edificacao,
            pf.regime,
            pf.publico_tradicional_estimado,
            ts.situacao_ts,
            at.pc_obra,
            at.qt_pesquisas_sigdh,
            at.pc_pesquisas_sigdh,
            at.qt_dossies_enviados,
            at.qt_dossies_aprovados,
            at.pc_dossies_aprovados,
            at.qt_contratos_assinados,
            at.status_equip_comunitario_50,
            at.status_infra_rede_eletrica,
            at.status_infra_pavimentacao,
            at.status_infra_agua_esgoto,
            at.pc_obra - an.pc_obra as var_pc_obra,
            at.qt_pesquisas_sigdh - an.qt_pesquisas_sigdh as var_qt_pesquisas_sigdh,
            at.qt_dossies_enviados - an.qt_dossies_enviados as var_qt_dossies_enviados,
            at.qt_dossies_aprovados - an.qt_dossies_aprovados as var_qt_dossies_aprovados,
            an.dt_referencia as dt_referencia_anterior,
            at.dt_referencia
        from atual at
        left join anterior an on an.apf = at.apf
        left join cadastro cad on cad.apf = at.apf
        left join ts on ts.apf = at.apf
        left join perfil pf on pf.apf = at.apf
    )

select
    j.*,
    case when j.qt_uh > 0 then round(j.qt_dossies_enviados::numeric / j.qt_uh * 100, 1) end::numeric(7, 1)
        as pc_dossies_enviados_por_uh,
    coalesce(j.pc_obra, 0) >= 50 and coalesce(j.qt_pesquisas_sigdh, 0) = 0 as alerta_50_sem_demanda,
    coalesce(j.pc_obra, 0) >= 60 and coalesce(j.qt_dossies_enviados, 0) = 0 as alerta_60_sem_dossie,
    case
        when coalesce(j.pc_obra, 0) >= 60 and coalesce(j.qt_pesquisas_sigdh, 0) = 0
            then 'Obra a partir de 60% sem famílias indicadas nem dossiês'
        when coalesce(j.pc_obra, 0) >= 60 and coalesce(j.qt_dossies_enviados, 0) = 0
            then 'Obra a partir de 60% com famílias indicadas, mas sem dossiê enviado'
        when coalesce(j.pc_obra, 0) >= 50 and coalesce(j.qt_pesquisas_sigdh, 0) = 0
            then 'Obra entre 50% e 60% sem famílias indicadas'
        when coalesce(j.qt_dossies_enviados, 0) > 0 then 'Com dossiês enviados'
        when coalesce(j.qt_pesquisas_sigdh, 0) > 0 then 'Com famílias indicadas, sem dossiê (obra abaixo de 60%)'
        else 'Sem famílias indicadas (obra abaixo de 50%)'
    end as situacao_demanda,
    case
        when coalesce(j.pc_obra, 0) >= 60 and coalesce(j.qt_pesquisas_sigdh, 0) = 0 then 1
        when coalesce(j.pc_obra, 0) >= 60 and coalesce(j.qt_dossies_enviados, 0) = 0 then 2
        when coalesce(j.pc_obra, 0) >= 50 and coalesce(j.qt_pesquisas_sigdh, 0) = 0 then 3
        else 9
    end as prioridade
from junto j
