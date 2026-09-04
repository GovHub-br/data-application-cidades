{{ config(materialized="table") }}

-- Silver: Empreendimento Rural (PNHR / MCMV Rural) — visão unificada por APF.
-- MESMO contrato de colunas de saída de silver_fds_empreendimento (D7 da change
-- migracao-bronze-minio-mcmv) — colunas sem equivalente no Rural saem NULL/default.
-- Espinha: bronze_rural_cadastro_pj (Novo MCMV Rural, ~127 empreendimentos).
-- Enriquecida por obra, financeiro (raiz de 6), INT065 CAIXA e SNH (snapshot corrente).
-- Grão: 1 linha por APF.
with
    cadastro as (select * from {{ ref("bronze_rural_cadastro_pj") }}),

    obra as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by apf order by dt_movimento desc nulls last
                    ) as rn
                from {{ ref("bronze_rural_obra_mensal") }}
            ) t
        where rn = 1
    ),

    int065 as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by apf order by dt_movimento desc nulls last
                    ) as rn
                from {{ ref("bronze_rural_int065_caixa") }}
                where apf is not null
            ) t
        where rn = 1
    ),

    snh as (select * from {{ ref("bronze_rural_dados_prioritarios_snh") }}),

    desembolso_acumulado as (
        select
            right(apf, 6) as apf_raiz,
            sum(
                coalesce(vr_desembolso_obra, 0)
                + coalesce(vr_desembolso_trabalho_social, 0)
                + coalesce(vr_desembolso_atec, 0)
                + coalesce(vr_desembolso_cisternas_efluentes, 0)
                + coalesce(vr_desembolso_custos_indiretos, 0)
            ) as vr_total_desembolsado,
            count(*) as qt_liberacoes_total,
            max(dt_liberacao) as dt_ultima_liberacao
        from {{ ref("bronze_rural_financeiro_mensal") }}
        where dt_liberacao is not null
        group by right(apf, 6)
    )

select
    c.apf,
    md5('empreendimento-rural|' || c.apf) as id_empreendimento,
    'Obra' as fase_empreendimento,

    -- Entidade Organizadora
    c.eo_nome,
    lpad(c.eo_cnpj, 14, '0') as eo_cnpj,
    null::int as co_nivel_hab_eo,
    c.ic_substituicao_eo,
    null::date as dt_substituicao_eo,
    c.eo_substituta_nome,
    c.eo_substituta_cnpj,

    -- Empreendimento
    coalesce(
        c.empreendimento_nome, i.empreendimento_nome, sn.empreendimento_nome
    ) as empreendimento_nome,
    null::text as construtora_nome,
    null::text as construtora_cnpj,
    coalesce(c.agente_financeiro, i.agente_financeiro) as agente_financeiro,
    greatest(
        coalesce(c.qt_uh_contratadas, 0), coalesce(c.qt_uh_selecionadas, 0)
    ) as quantidade_uh,
    c.qt_uh_contratadas as qt_uh_construcao,
    c.qt_uh_selecionadas as qt_uh_projeto,
    floor(
        greatest(coalesce(c.qt_uh_contratadas, 0), coalesce(c.qt_uh_selecionadas, 0))
        * 3.3
    )::int as pessoas_atendidas,

    -- Localização
    coalesce(c.municipio, i.municipio, sn.municipio) as municipio,
    coalesce(c.uf, i.uf, sn.uf) as uf,
    coalesce(c.cod_ibge, i.cod_ibge, sn.cod_ibge) as cod_ibge,

    -- Tipologia (não decodificada no Rural)
    null::int as co_tipo_edificacao,
    'Não Informado' as tipologia,
    null::int as co_regime_obra,
    c.co_modalidade,

    -- Valores contratuais
    coalesce(c.vr_total_investimento, i.vr_total_investimento, 0.0) as valor_contratado,
    coalesce(c.vr_emprestimo, i.vr_emprestimo, 0.0) as valor_financiamento_fds,
    coalesce(
        c.vr_total_contrapartidas, i.vr_total_contrapartidas, 0.0
    ) as valor_contrapartidas,
    case
        when
            greatest(coalesce(c.qt_uh_contratadas, 0), coalesce(c.qt_uh_selecionadas, 0))
            > 0
        then
            coalesce(c.vr_total_investimento, i.vr_total_investimento, 0.0) / greatest(
                coalesce(c.qt_uh_contratadas, 0), coalesce(c.qt_uh_selecionadas, 0)
            )
        else 0.0
    end as valor_por_uh,

    -- Datas do contrato
    nullif(c.dt_contratacao, '1900-01-01'::date) as dt_contratacao,
    null::date as dt_inicio_obra,
    null::date as dt_previsao_conclusao,

    -- Situação
    coalesce(i.situacao_obra, sn.situacao, 'Não mapeado') as situacao_gefus,
    'Não mapeada' as fase_contrato,
    o.co_situacao_operacao,
    o.co_andamento_operacao,

    -- Evolução física
    coalesce(o.pct_obra_prevista, 0.0) as percentual_obra_prevista,
    coalesce(
        o.pct_obra_realizada,
        c.pct_obra_realizada,
        i.pct_obra_realizada,
        sn.pct_execucao,
        0.0
    ) as percentual_execucao_fisica,

    -- UHs
    coalesce(
        o.qt_uh_concluidas, c.qt_uh_concluidas, i.qt_uh_concluidas, 0
    ) as qt_uh_concluidas,
    coalesce(o.qt_uh_alienadas, i.qt_uh_entregues, 0) as qt_uh_alienadas,
    coalesce(o.qt_uh_sem_habitese, 0) as qt_uh_sem_habitese,
    coalesce(o.qt_uh_construcao_parcial, 0) as qt_uh_construcao_parcial,
    coalesce(o.qt_uh_ocupacao_irregular, 0) as qt_uh_ocupacao_irregular,
    case
        when
            greatest(coalesce(c.qt_uh_contratadas, 0), coalesce(c.qt_uh_selecionadas, 0))
            > 0
        then
            round(
                coalesce(o.qt_uh_alienadas, i.qt_uh_entregues, 0)::numeric / greatest(
                    coalesce(c.qt_uh_contratadas, 0), coalesce(c.qt_uh_selecionadas, 0)
                )
                * 100,
                2
            )
        else 0.0
    end as pct_entrega,

    -- Paralisação / alertas
    o.dt_paralisacao,
    o.co_classificacao_paralisado,
    coalesce(o.ic_invadido, false) as ic_invadido,
    o.dt_invasao,

    -- Marcos
    coalesce(
        o.dt_conclusao_obra, c.dt_conclusao_obra, i.dt_conclusao_obra
    ) as dt_conclusao_obra,
    o.dt_entrega,
    o.dt_previsao_entrega,

    -- Evolução financeira
    coalesce(
        d.vr_total_desembolsado, c.vr_liberado, i.vr_liberado, 0.0
    ) as valor_desembolsado,
    coalesce(d.qt_liberacoes_total, 0) as qt_liberacoes,
    coalesce(
        d.dt_ultima_liberacao, c.dt_ultima_liberacao, i.dt_ultima_liberacao
    ) as dt_ultima_liberacao,
    case
        when coalesce(c.vr_total_investimento, i.vr_total_investimento, 0.0) > 0
        then
            round(
                coalesce(d.vr_total_desembolsado, c.vr_liberado, i.vr_liberado, 0.0)
                / coalesce(c.vr_total_investimento, i.vr_total_investimento)
                * 100,
                2
            )
        else 0.0
    end as percentual_execucao_financeira,
    case
        when coalesce(c.vr_total_investimento, i.vr_total_investimento, 0.0) > 0
        then
            round(
                coalesce(d.vr_total_desembolsado, c.vr_liberado, i.vr_liberado, 0.0)
                / coalesce(c.vr_total_investimento, i.vr_total_investimento)
                * 100
                - coalesce(o.pct_obra_realizada, c.pct_obra_realizada, 0.0),
                2
            )
        else 0.0
    end as divergencia_fisico_financeira,

    -- Trabalho social (sem tabela mensal dedicada no Rural)
    null::int as co_situacao_trabalho_social,
    null::numeric(6, 2) as pct_execucao_ts,
    null::date as dt_aprovacao_pts,

    -- Tempo
    null::int as dias_contratacao_inicio,

    -- GPS (não disponível no cadastro PJ Rural)
    null::numeric as gps_lat_grau,
    null::numeric as gps_lat_minuto,
    null::numeric as gps_lat_segundo,
    null::numeric as gps_long_grau,
    null::numeric as gps_long_minuto,
    null::numeric as gps_long_segundo,

    -- Enriquecimento SNH (snapshot 30/09/2025)
    (sn.apf is not null) as tem_dados_snh,
    sn.situacao as snh_situacao,
    sn.situacao_agrupada as snh_situacao_agrupada,
    null::text as snh_apf_fase_obra,
    sn.pct_execucao as snh_percentual_obra,
    sn.valor_contratado_total as snh_valor_contratado_total,
    sn.valor_desembolsado as snh_valor_desembolsado,
    sn.uh_contratadas as snh_uh_contratadas,
    sn.uh_entregues as snh_uh_entregues,
    sn.uh_vigentes as snh_uh_vigentes,
    sn.dt_termino as snh_dt_termino,
    sn.dt_previsao_termino as snh_dt_previsao_termino

from cadastro c
left join obra o on c.apf = o.apf
left join int065 i on c.apf = i.apf
left join snh sn on c.apf = sn.apf
left join desembolso_acumulado d on left(c.apf, 6) = d.apf_raiz
