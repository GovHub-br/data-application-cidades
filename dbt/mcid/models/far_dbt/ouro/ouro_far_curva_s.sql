{{ config(materialized="table") }}

-- Ouro: Curva S do FAR — evolução física e financeira agregada por mês, no nível
-- nacional e por UF, discriminados por `nivel`.
-- Os percentuais são ponderados pelo valor FAR de cada APF, para que obra grande pese
-- mais que obra pequena. Fora da curva: APF cuja situação mais recente na CAIXA é
-- DISTRATADO/CANCELADO (ficaria em 0% para sempre, puxando a curva para baixo).
-- A carteira cresce com as contratações, então cada mês mede os APFs já existentes.
-- APF sem valor FAR (fora do cadastro MONIT) soma nos valores, mas não nos percentuais.
with
    serie as (select * from {{ ref("prata_far_serie_mensal") }}),

    situacao_atual as (
        select distinct on (apf) apf, situacao
        from {{ ref("prata_far_caixa_serie") }}
        order by apf, competencia desc
    ),

    carteira as (
        select s.*
        from serie s
        left join situacao_atual a on a.apf = s.apf
        where coalesce(a.situacao, '') != 'DISTRATADO/CANCELADO'
    ),

    agregado as (
        select
            mes,
            case when grouping(uf) = 1 then 'nacional' else 'uf' end as nivel,
            case when grouping(uf) = 1 then 'BR' else uf end as uf,

            count(*) as qt_empreendimentos,
            count(pct_obra_realizada) as qt_com_fisico,
            count(pct_obra_prevista) as qt_com_previsto,
            sum(vr_emprestimo_far) as vr_emprestimo_far,

            -- Físico ponderado pelo valor FAR, só entre quem tem o dado
            sum(pct_obra_realizada * vr_emprestimo_far)
            / nullif(
                sum(vr_emprestimo_far) filter (where pct_obra_realizada is not null), 0
            ) as pct_obra_realizada,
            sum(pct_obra_prevista * vr_emprestimo_far)
            / nullif(
                sum(vr_emprestimo_far) filter (where pct_obra_prevista is not null), 0
            ) as pct_obra_prevista,

            -- Financeiro
            sum(vr_liberado_mes) as vr_liberado_mes,
            sum(vr_liberado_acum) as vr_liberado_acum,
            sum(vr_liberado_acum) filter (where vr_emprestimo_far is not null)
            / nullif(sum(vr_emprestimo_far), 0)
            * 100 as pct_financeiro,

            -- Conciliação com a CAIXA
            sum(vr_desembolsado_caixa) as vr_desembolsado_caixa

        from carteira
        group by grouping sets ((mes), (mes, uf))
    )

select
    mes,
    to_char(mes, 'YYYY-MM') as mes_label,
    to_char(mes, 'MM/YYYY') as mes_label_br,
    nivel,
    uf,
    qt_empreendimentos,
    qt_com_fisico,
    qt_com_previsto,
    round(vr_emprestimo_far, 2) as vr_emprestimo_far,
    round(pct_obra_realizada, 2) as pct_obra_realizada,
    round(pct_obra_prevista, 2) as pct_obra_prevista,
    round(vr_liberado_mes, 2) as vr_liberado_mes,
    round(vr_liberado_acum, 2) as vr_liberado_acum,
    round(pct_financeiro, 2) as pct_financeiro,
    round(vr_desembolsado_caixa, 2) as vr_desembolsado_caixa
from agregado
-- APF sem UF entra no nacional, mas não vira uma "UF nula"
where uf is not null
