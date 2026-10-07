{{ config(materialized="table") }}

-- Prata: Série mensal de execução física e financeira — uma linha por APF × mês.
-- APFs: os do cadastro MONIT, mais os que têm liberação no livro-razão mas não têm
-- cadastro (identificados pela CAIXA; sem valor FAR, então sem % financeiro).
-- Calendário contínuo do primeiro sinal do APF (contratação, liberação ou medição) até
-- a última competência disponível em qualquer fonte.
--   Financeiro: livro-razão MONIT (valor do mês e acumulado); % sobre o valor FAR.
--   Físico: MONIT; sem MONIT no mês, CAIXA; sem nenhum, o último valor arrastado.
--           `fonte_fisica` diz de onde veio cada ponto. A CAIXA manda `exec` = 0 para
--           o Novo MCMV antes de começar a medi-lo, então só vale a partir da primeira
--           competência com algum APF do cadastro acima de zero.
--   Previsto: só o MONIT informa. Mês sem MONIT entre dois observados é interpolado
--           em linha reta; depois do último observado, o último valor é arrastado.
--           `fonte_prevista` diz qual dos casos.
--   CAIXA: colunas `_caixa` como vieram no mês, só para conciliação.
with
    cadastro as (
        select
            apf,
            empreendimento_nome,
            municipio,
            uf,
            dt_contratacao,
            vr_emprestimo_far,
            vr_total_investimento
        from {{ ref("prata_far_cadastro_pj") }}
    ),

    obra as (select * from {{ ref("prata_far_obra_serie") }}),

    caixa as (select * from {{ ref("prata_far_caixa_serie") }}),

    situacao_obra as (select * from {{ ref("far_situacao_obra") }}),

    -- O financeiro identifica o APF pelos 6 primeiros dígitos
    financeiro as (
        select
            right(apf, 6) as apf_raiz,
            date_trunc('month', dt_liberacao)::date as mes,
            count(*) as qt_liberacoes,
            sum(vr_liberado) as vr_liberado_mes,
            sum(vr_pago_obra) as vr_pago_obra_mes,
            sum(vr_pago_incc) as vr_pago_incc_mes
        from {{ ref("prata_far_financeiro_mensal") }}
        where dt_liberacao is not null
        group by 1, 2
    ),

    -- Identificação mais recente na CAIXA, para quem não está no cadastro
    caixa_atual as (
        select distinct on (apf) apf, empreendimento_nome, municipio, uf
        from caixa
        order by apf, competencia desc
    ),

    universo as (
        select apf, true as ic_cadastro_monit
        from cadastro
        union all
        select x.apf, false
        from caixa_atual x
        where
            left(x.apf, 6) in (select apf_raiz from financeiro)
            and left(x.apf, 6) not in (select left(apf, 6) from cadastro)
    ),

    inicio_exec_caixa as (
        select min(x.competencia) as mes
        from caixa x
        inner join cadastro c on c.apf = x.apf
        where x.pct_execucao > 0
    ),

    ultimo_mes as (
        select max(mes) as mes
        from
            (
                select max(competencia) as mes from obra
                union all
                select max(competencia) from caixa
                union all
                select max(mes) from financeiro
            ) t
    ),

    inicio as (
        select
            u.apf,
            least(
                date_trunc('month', c.dt_contratacao)::date, f.mes, o.mes, x.mes
            ) as mes
        from universo u
        left join cadastro c on c.apf = u.apf
        left join
            (select apf_raiz, min(mes) as mes from financeiro group by 1) f
            on f.apf_raiz = left(u.apf, 6)
        left join
            (select apf, min(competencia) as mes from obra group by 1) o
            on o.apf = u.apf
        left join
            (select apf, min(competencia) as mes from caixa group by 1) x
            on x.apf = u.apf
    ),

    calendario as (
        select i.apf, gs::date as mes
        from inicio i
        cross join ultimo_mes u
        cross join lateral generate_series(i.mes, u.mes, interval '1 month') as gs
        where i.mes is not null
    ),

    observado as (
        select
            cal.apf,
            cal.mes,

            -- Financeiro
            coalesce(f.qt_liberacoes, 0) as qt_liberacoes,
            coalesce(f.vr_liberado_mes, 0.0) as vr_liberado_mes,
            coalesce(f.vr_pago_obra_mes, 0.0) as vr_pago_obra_mes,
            coalesce(f.vr_pago_incc_mes, 0.0) as vr_pago_incc_mes,
            sum(coalesce(f.vr_liberado_mes, 0.0)) over (
                partition by cal.apf order by cal.mes
            ) as vr_liberado_acum,
            sum(coalesce(f.vr_pago_incc_mes, 0.0)) over (
                partition by cal.apf order by cal.mes
            ) as vr_pago_incc_acum,

            -- Físico observado no mês
            coalesce(o.pct_obra_realizada, xe.pct_execucao) as pct_realizada_obs,
            case
                when o.pct_obra_realizada is not null
                then 'monit'
                when xe.pct_execucao is not null
                then 'caixa'
            end as fonte_obs,
            o.pct_obra_prevista as pct_prevista_obs,
            o.co_situacao_obra as co_situacao_obs,

            -- CAIXA no mês
            x.situacao as situacao_caixa,
            x.pct_execucao as pct_execucao_caixa,
            x.valor_contratado as valor_contratado_caixa,
            x.valor_desembolsado as vr_desembolsado_caixa,
            o.pct_obra_realizada as pct_obra_realizada_monit

        from calendario cal
        left join financeiro f on f.apf_raiz = left(cal.apf, 6) and f.mes = cal.mes
        left join obra o on o.apf = cal.apf and o.competencia = cal.mes
        left join caixa x on x.apf = cal.apf and x.competencia = cal.mes
        left join
            caixa xe
            on xe.apf = cal.apf
            and xe.competencia = cal.mes
            and xe.competencia >= (select mes from inicio_exec_caixa)
    ),

    -- LOCF passo 1: o count() acumulado abre um grupo a cada valor observado
    agrupado as (
        select
            *,
            count(pct_realizada_obs) over (
                partition by apf order by mes
            ) as grp_realizada,
            count(pct_prevista_obs) over (partition by apf order by mes) as grp_prevista,
            -- Contado de trás para frente, agrupa cada mês com o PRÓXIMO observado
            count(pct_prevista_obs) over (
                partition by apf order by mes desc
            ) as grp_prevista_prox,
            count(co_situacao_obs) over (partition by apf order by mes) as grp_situacao
        from observado
    ),

    -- LOCF passo 2: dentro do grupo, o primeiro valor é o último observado
    preenchido as (
        select
            *,
            first_value(pct_realizada_obs) over (
                partition by apf, grp_realizada order by mes
            ) as pct_obra_realizada,
            first_value(pct_prevista_obs) over (
                partition by apf, grp_prevista order by mes
            ) as prevista_ant,
            first_value(mes) over (
                partition by apf, grp_prevista order by mes
            ) as mes_prevista_ant,
            first_value(pct_prevista_obs) over (
                partition by apf, grp_prevista_prox order by mes desc
            ) as prevista_prox,
            first_value(mes) over (
                partition by apf, grp_prevista_prox order by mes desc
            ) as mes_prevista_prox,
            first_value(co_situacao_obs) over (
                partition by apf, grp_situacao order by mes
            ) as co_situacao_obra
        from agrupado
    ),

    previsto as (
        select
            *,
            case
                when pct_prevista_obs is not null
                then pct_prevista_obs
                when prevista_ant is not null and prevista_prox is not null
                then
                    round(
                        prevista_ant
                        + (prevista_prox - prevista_ant)
                        -- meses desde o anterior / meses entre anterior e próximo
                        * (
                            extract(year from age(mes, mes_prevista_ant)) * 12
                            + extract(month from age(mes, mes_prevista_ant))
                        )
                        / (
                            extract(year from age(mes_prevista_prox, mes_prevista_ant))
                            * 12
                            + extract(month from age(mes_prevista_prox, mes_prevista_ant))
                        ),
                        2
                    )
                else prevista_ant
            end as pct_obra_prevista,
            case
                when pct_prevista_obs is not null
                then 'monit'
                when prevista_ant is not null and prevista_prox is not null
                then 'interpolado'
                when prevista_ant is not null
                then 'carregado'
            end as fonte_prevista
        from preenchido
    )

select
    p.apf,
    p.mes,

    -- Identificação
    coalesce(c.empreendimento_nome, a.empreendimento_nome) as empreendimento_nome,
    coalesce(c.municipio, a.municipio) as municipio,
    coalesce(c.uf, a.uf) as uf,
    u.ic_cadastro_monit,

    -- Execução física
    p.pct_obra_realizada,
    p.pct_obra_prevista,
    p.pct_obra_realizada - p.pct_obra_prevista as desvio_fisico,
    case
        when p.pct_realizada_obs is not null
        then p.fonte_obs
        when p.grp_realizada > 0
        then 'carregado'
    end as fonte_fisica,
    p.fonte_prevista,

    -- Situação
    p.co_situacao_obra,
    s.situacao_obra,

    -- Execução financeira
    p.qt_liberacoes,
    p.vr_liberado_mes,
    p.vr_pago_obra_mes,
    p.vr_pago_incc_mes,
    p.vr_liberado_acum,
    p.vr_pago_incc_acum,
    c.vr_emprestimo_far,
    c.vr_total_investimento,
    case
        when c.vr_emprestimo_far > 0
        then round(p.vr_liberado_acum / c.vr_emprestimo_far * 100, 2)
    end as pct_financeiro,

    -- Conciliação com a CAIXA
    p.situacao_caixa,
    p.pct_execucao_caixa,
    p.valor_contratado_caixa,
    p.vr_desembolsado_caixa,
    case
        when p.valor_contratado_caixa > 0
        then round(p.vr_desembolsado_caixa / p.valor_contratado_caixa * 100, 2)
    end as pct_financeiro_caixa,
    p.pct_obra_realizada_monit - p.pct_execucao_caixa as dif_pct_fisico_caixa,
    p.vr_liberado_acum - p.vr_desembolsado_caixa as dif_desembolso_caixa

from previsto p
inner join universo u on u.apf = p.apf
left join cadastro c on c.apf = p.apf
left join caixa_atual a on a.apf = p.apf
left join situacao_obra s on s.co_situacao_obra = p.co_situacao_obra
