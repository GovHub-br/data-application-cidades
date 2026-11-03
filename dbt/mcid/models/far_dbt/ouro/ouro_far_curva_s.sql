{{ config(materialized="table") }}

-- Ouro: Curva S do FAR — evolução física e financeira agregada por mês, no nível
-- nacional e por UF, discriminados por `nivel`.
-- Os percentuais são ponderados pelo valor FAR de cada APF, para que obra grande pese
-- mais que obra pequena, e divididos pelo valor FAR de toda a carteira do recorte em
-- todos os meses: obra que ainda não começou conta como 0%, e a curva não cai quando
-- outra começa. A carteira é a da carga mais recente, então o histórico se recalcula
-- quando entra contrato novo. A curva começa no primeiro mês com físico realizado no
-- recorte; antes disso tudo está em cláusula suspensiva ou aguardando início.
-- Fora da carteira: APF cuja situação mais recente na CAIXA é DISTRATADO/CANCELADO.
-- APF sem valor FAR (fora do cadastro MONIT) soma nos valores, mas não nos percentuais.
-- Cada APF entra na sua portaria (planilha do FAR) e em 'Todas as portarias', e no seu
-- município e em 'Todos os municípios', para os filtros do dashboard não somarem
-- recortes. O nacional só existe em 'Todos os municípios'. O município vem da ficha,
-- com a grafia que o filtro oferece.
with
    serie as (select * from {{ ref("prata_far_serie_mensal") }}),

    situacao_atual as (
        select distinct on (apf) apf, situacao
        from {{ ref("prata_far_caixa_serie") }}
        order by apf, competencia desc
    ),

    portaria_apf as (select distinct apf, portaria from {{ ref("far_portaria_apf") }}),

    ficha as (select apf, municipio from {{ ref("ouro_far_ficha_empreendimento") }}),

    carteira as (
        select s.*, p.portaria, m.municipio as recorte_municipio
        from serie s
        left join situacao_atual a on a.apf = s.apf
        left join portaria_apf pa on pa.apf = s.apf
        left join ficha f on f.apf = s.apf
        cross join lateral (
            values (coalesce(pa.portaria, 'Não informada')), ('Todas as portarias')
        ) p(portaria)
        cross join lateral (
            values (coalesce(f.municipio, 'Não informado')), ('Todos os municípios')
        ) m(municipio)
        where coalesce(a.situacao, '') != 'DISTRATADO/CANCELADO'
    ),

    -- Denominador fixo e início da curva de cada recorte
    recorte as (
        select
            case when grouping(uf) = 1 then 'nacional' else 'uf' end as nivel,
            case when grouping(uf) = 1 then 'BR' else uf end as uf,
            portaria,
            recorte_municipio as municipio,
            count(*) as qt_empreendimentos,
            sum(vr_emprestimo_far) as vr_emprestimo_far,
            min(mes_inicio_fisico) as mes_inicio
        from
            (
                select distinct
                    apf,
                    uf,
                    portaria,
                    recorte_municipio,
                    vr_emprestimo_far,
                    mes_inicio_fisico
                from carteira
            ) apfs
        group by
            grouping sets (
                (portaria, recorte_municipio), (uf, portaria, recorte_municipio)
            )
    ),

    agregado as (
        select
            mes,
            case when grouping(uf) = 1 then 'nacional' else 'uf' end as nivel,
            case when grouping(uf) = 1 then 'BR' else uf end as uf,
            portaria,
            recorte_municipio as municipio,

            count(*) filter (where mes >= mes_inicio_fisico) as qt_iniciados,
            count(pct_obra_prevista) as qt_com_previsto,
            sum(pct_obra_realizada * vr_emprestimo_far) as realizado_ponderado,
            sum(pct_obra_prevista * vr_emprestimo_far) as previsto_ponderado,
            sum(vr_liberado_mes) as vr_liberado_mes,
            sum(vr_liberado_acum) as vr_liberado_acum,
            sum(vr_liberado_acum) filter (
                where vr_emprestimo_far is not null
            ) as liberado_com_valor_far,

            -- Conciliação com a CAIXA
            sum(vr_desembolsado_caixa) as vr_desembolsado_caixa

        from carteira
        group by
            grouping sets (
                (mes, portaria, recorte_municipio),
                (mes, uf, portaria, recorte_municipio)
            )
    )

select
    a.mes,
    to_char(a.mes, 'YYYY-MM') as mes_label,
    to_char(a.mes, 'MM/YYYY') as mes_label_br,
    a.nivel,
    a.uf,
    a.portaria,
    a.municipio,
    r.qt_empreendimentos,
    a.qt_iniciados,
    a.qt_com_previsto,
    round(r.vr_emprestimo_far, 2) as vr_emprestimo_far,
    round(
        coalesce(a.realizado_ponderado, 0) / nullif(r.vr_emprestimo_far, 0), 2
    ) as pct_obra_realizada,
    -- Previsto só existe a partir do primeiro arquivo de obra do MONIT
    case
        when a.qt_com_previsto > 0
        then round(a.previsto_ponderado / nullif(r.vr_emprestimo_far, 0), 2)
    end as pct_obra_prevista,
    round(a.vr_liberado_mes, 2) as vr_liberado_mes,
    round(a.vr_liberado_acum, 2) as vr_liberado_acum,
    round(
        coalesce(a.liberado_com_valor_far, 0) / nullif(r.vr_emprestimo_far, 0) * 100, 2
    ) as pct_financeiro,
    round(a.vr_desembolsado_caixa, 2) as vr_desembolsado_caixa
from agregado a
inner join
    recorte r
    on r.nivel = a.nivel
    and r.uf = a.uf
    and r.portaria = a.portaria
    and r.municipio = a.municipio
where
    a.mes >= r.mes_inicio
    -- APF sem UF entra no nacional, mas não vira uma "UF nula"
    and a.uf is not null
    and (a.nivel = 'uf' or a.municipio = 'Todos os municípios')
