{{ config(materialized="table", alias="silver_atual_evolucao_financeira") }}

-- Silver: Evolução Financeira FDS — liberações CONHECIDAS NO SNAPSHOT do
-- SharePoint, agrupadas pelo mês da liberação. NÃO é a série de desembolsos do
-- empreendimento (cobre só APF pós-2024; decomposição por componente, não o
-- total acumulado — feed GEFUS/INT; ver glossario-valores-financeiros.md, D4).
-- Agrega as liberações da bronze fds_financeiro_mensal por APF e mês.
-- OBS: No FDS, ic_credito='0' são liberações reais (valores negativos), usar ABS().
-- JOIN com fds_empreendimento usando raiz de 6 dígitos.
-- Grão: 1 linha por APF × mês (relação 1:N com empreendimento)
with
    financeiro as (select * from {{ ref("bronze_fds_financeiro_mensal") }}),

    empreendimento as (select * from {{ ref("prata_fds_empreendimento") }}),

    -- Agregação mensal: soma todas as liberações reais do mês por APF
    -- Filtra apenas ic_credito='0' (liberações reais) e aplica ABS()
    mensal as (
        select
            -- Chave raiz de 6 dígitos para JOIN posterior
            right(f.apf, 6) as apf_raiz,

            -- Mês da liberação (truncado ao primeiro dia)
            date_trunc('month', f.dt_liberacao) as mes,

            -- Totais do mês
            count(*) as qt_liberacoes,
            sum(abs(f.vr_liberado)) as vr_liberado_mes,

            -- Decomposição por componente
            sum(abs(f.vr_pago_obra)) as vr_pago_obra_mes,
            sum(abs(f.vr_pago_terreno)) as vr_pago_terreno_mes,
            sum(abs(f.vr_pago_trabalho_social)) as vr_pago_pts_mes,
            sum(abs(f.vr_pago_projeto)) as vr_pago_projeto_mes,
            sum(abs(f.vr_pago_incc)) as vr_pago_incc_mes,
            sum(abs(f.vr_pago_aporte)) as vr_pago_aporte_mes,
            sum(abs(f.vr_pago_legalizacao)) as vr_pago_legalizacao_mes,
            sum(abs(f.vr_pago_seguranca)) as vr_pago_seguranca_mes

        from financeiro f
        where
            f.ic_credito = '0'  -- apenas liberações reais
            and f.dt_liberacao is not null
            and f.vr_liberado is not null
        group by right(f.apf, 6), date_trunc('month', f.dt_liberacao)
    ),

    -- JOIN com empreendimento via raiz 6 dígitos
    -- e cálculo do acumulado com window function
    evolucao as (
        select
            e.apf,
            e.eo_cnpj,
            e.eo_nome,
            m.mes,
            m.qt_liberacoes,

            -- Valores mensais
            coalesce(m.vr_liberado_mes, 0.0) as vr_liberado_mes,
            coalesce(m.vr_pago_obra_mes, 0.0) as vr_pago_obra_mes,
            coalesce(m.vr_pago_terreno_mes, 0.0) as vr_pago_terreno_mes,
            coalesce(m.vr_pago_pts_mes, 0.0) as vr_pago_pts_mes,
            coalesce(m.vr_pago_projeto_mes, 0.0) as vr_pago_projeto_mes,
            coalesce(m.vr_pago_incc_mes, 0.0) as vr_pago_incc_mes,
            coalesce(m.vr_pago_aporte_mes, 0.0) as vr_pago_aporte_mes,
            coalesce(m.vr_pago_legalizacao_mes, 0.0) as vr_pago_legalizacao_mes,
            coalesce(m.vr_pago_seguranca_mes, 0.0) as vr_pago_seguranca_mes,

            -- Acumulado progressivo por APF
            sum(m.vr_liberado_mes) over (
                partition by e.apf order by m.mes
            ) as vr_acumulado,

            -- Contexto do empreendimento (para cálculo de %)
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

    -- Valores mensais
    vr_liberado_mes,
    vr_pago_obra_mes,
    vr_pago_terreno_mes,
    vr_pago_pts_mes,
    vr_pago_projeto_mes,
    vr_pago_incc_mes,
    vr_pago_aporte_mes,
    vr_pago_legalizacao_mes,
    vr_pago_seguranca_mes,
    -- Residual explícito (change vocabulario-e-qualidade-financeira-historica,
    -- D5): o feed SharePoint FDS só decompõe ~19% do vr_liberado (80% das
    -- linhas têm todos os componentes NULL). vr_pago_outros_mes fecha a
    -- decomposição com o total. greatest(...,0) evita residual negativo por
    -- arredondamento. Testado por reconcilia_decomposicao.
    greatest(
        vr_liberado_mes - (
            vr_pago_obra_mes + vr_pago_terreno_mes + vr_pago_pts_mes
            + vr_pago_projeto_mes + vr_pago_incc_mes + vr_pago_aporte_mes
            + vr_pago_legalizacao_mes + vr_pago_seguranca_mes
        ),
        0.0
    ) as vr_pago_outros_mes,

    -- Acumulado e percentual
    vr_acumulado,
    case
        when coalesce(valor_contratado, 0.0) > 0
        then round((vr_acumulado / valor_contratado) * 100, 2)
        else 0.0
    end as pct_executado_financeiro,

    -- Contexto
    valor_contratado,
    municipio,
    uf

from evolucao
