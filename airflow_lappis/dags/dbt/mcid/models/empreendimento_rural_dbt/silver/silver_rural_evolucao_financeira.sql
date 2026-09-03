{{ config(materialized="table") }}

-- Silver: Evolução Financeira Rural — série temporal mensal de desembolsos por APF.
-- Espelho de silver_fds_evolucao_financeira (mesmo contrato de saída); componentes
-- sem equivalente no Rural (terreno, projeto, INCC, legalização, segurança, aporte)
-- saem NULL. JOIN com silver_rural_empreendimento pela raiz de 6 dígitos do APF.
-- Grão: 1 linha por APF × mês.

with
    financeiro as (
        select * from {{ ref("bronze_rural_financeiro_mensal") }}
    ),

    empreendimento as (
        select * from {{ ref("silver_rural_empreendimento") }}
    ),

    mensal as (
        select
            right(f.apf, 6) as apf_raiz,
            date_trunc('month', f.dt_liberacao) as mes,
            count(*) as qt_liberacoes,
            sum(
                coalesce(f.vr_desembolso_obra, 0) + coalesce(f.vr_desembolso_trabalho_social, 0)
                + coalesce(f.vr_desembolso_atec, 0) + coalesce(f.vr_desembolso_cisternas_efluentes, 0)
                + coalesce(f.vr_desembolso_custos_indiretos, 0)
            ) as vr_liberado_mes,
            sum(coalesce(f.vr_desembolso_obra, 0)) as vr_pago_obra_mes,
            sum(coalesce(f.vr_desembolso_trabalho_social, 0)) as vr_pago_pts_mes
        from financeiro f
        where f.dt_liberacao is not null
        group by right(f.apf, 6), date_trunc('month', f.dt_liberacao)
    ),

    evolucao as (
        select
            e.apf,
            e.eo_cnpj,
            e.eo_nome,
            m.mes,
            m.qt_liberacoes,
            coalesce(m.vr_liberado_mes, 0.0) as vr_liberado_mes,
            coalesce(m.vr_pago_obra_mes, 0.0) as vr_pago_obra_mes,
            null::numeric(15, 2) as vr_pago_terreno_mes,
            coalesce(m.vr_pago_pts_mes, 0.0) as vr_pago_pts_mes,
            null::numeric(15, 2) as vr_pago_projeto_mes,
            null::numeric(15, 2) as vr_pago_incc_mes,
            null::numeric(15, 2) as vr_pago_aporte_mes,
            null::numeric(15, 2) as vr_pago_legalizacao_mes,
            null::numeric(15, 2) as vr_pago_seguranca_mes,
            sum(m.vr_liberado_mes) over (partition by e.apf order by m.mes) as vr_acumulado,
            e.valor_contratado,
            e.municipio,
            e.uf
        from mensal m
        inner join empreendimento e on m.apf_raiz = left(e.apf, 6)
    )

select
    apf,
    eo_cnpj,
    eo_nome,
    mes,
    qt_liberacoes,
    vr_liberado_mes,
    vr_pago_obra_mes,
    vr_pago_terreno_mes,
    vr_pago_pts_mes,
    vr_pago_projeto_mes,
    vr_pago_incc_mes,
    vr_pago_aporte_mes,
    vr_pago_legalizacao_mes,
    vr_pago_seguranca_mes,
    vr_acumulado,
    case
        when coalesce(valor_contratado, 0.0) > 0
        then round((vr_acumulado / valor_contratado) * 100, 2)
        else 0.0
    end as pct_executado_financeiro,
    valor_contratado,
    municipio,
    uf
from evolucao
