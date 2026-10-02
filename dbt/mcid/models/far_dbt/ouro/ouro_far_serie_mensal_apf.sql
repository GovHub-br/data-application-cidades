{{ config(materialized="table") }}

-- Ouro: Série mensal por APF — execução física e financeira prontas para o dashboard
-- e para o modelo preditivo. Acrescenta à prata a identificação do empreendimento e o
-- ritmo físico × financeiro do mês.
with
    serie as (select * from {{ ref("prata_far_serie_mensal") }}),

    ficha as (
        select apf, nome_empreendimento, apf_municipio_empreendimento
        from {{ ref("ouro_far_ficha_empreendimento") }}
    )

select
    -- Identificação (quem não está na ficha usa o nome da CAIXA)
    s.apf,
    coalesce(f.nome_empreendimento, upper(s.empreendimento_nome)) as nome_empreendimento,
    coalesce(
        f.apf_municipio_empreendimento,
        concat(
            s.apf, ' - ', s.municipio, '/', s.uf, ' - ', upper(s.empreendimento_nome)
        )
    ) as apf_municipio_empreendimento,
    s.municipio,
    s.uf,
    s.ic_cadastro_monit,

    -- Tempo
    s.mes,
    to_char(s.mes, 'YYYY-MM') as mes_label,
    to_char(s.mes, 'MM/YYYY') as mes_label_br,

    -- Execução física
    s.pct_obra_realizada,
    s.pct_obra_prevista,
    s.desvio_fisico,
    s.fonte_fisica,
    s.fonte_prevista,
    s.co_situacao_obra,
    s.situacao_obra,

    -- Execução financeira
    s.qt_liberacoes,
    round(s.vr_liberado_mes, 2) as vr_liberado_mes,
    round(s.vr_pago_obra_mes, 2) as vr_pago_obra_mes,
    round(s.vr_pago_incc_mes, 2) as vr_pago_incc_mes,
    round(s.vr_liberado_acum, 2) as vr_liberado_acum,
    round(s.vr_pago_incc_acum, 2) as vr_pago_incc_acum,
    round(s.vr_emprestimo_far, 2) as vr_emprestimo_far,
    s.pct_financeiro,

    -- Aderência do desembolso à obra (margem de 5 p.p., a mesma da ficha)
    case
        when s.pct_obra_realizada is null or s.pct_financeiro is null
        then null
        when s.pct_financeiro > s.pct_obra_realizada + 5
        then 'Desembolso Adiantado'
        when s.pct_financeiro < s.pct_obra_realizada - 5
        then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro,

    -- Conciliação com a CAIXA
    s.situacao_caixa,
    s.pct_execucao_caixa,
    round(s.valor_contratado_caixa, 2) as valor_contratado_caixa,
    round(s.vr_desembolsado_caixa, 2) as vr_desembolsado_caixa,
    s.pct_financeiro_caixa,
    s.dif_pct_fisico_caixa,
    round(s.dif_desembolso_caixa, 2) as dif_desembolso_caixa

from serie s
left join ficha f on f.apf = s.apf
