{{ config(materialized="table") }}

-- Silver: Empreendimento FDS — Visão unificada
-- Reúne dados cadastrais, EO, status físico-financeiro e TS de cada APF.
-- Cruza: fds_cadastro_pj + fds_obra_mensal + fds_int_059_caixa_pj + fds_trabalho_social
-- Alimenta: ficha_empreendimento, panorama_estadual, panorama_entidades (golds)

with
    cadastro as (
        select * from {{ ref("fds_cadastro_pj") }}
    ),

    obra as (
        select * from {{ ref("fds_obra_mensal") }}
    ),

    -- INT 059: filtrar apenas Novo MCMV-E e desduplicar por APF
    int059 as (
        select * from (
            select *,
                   row_number() over(partition by apf order by dt_movimento desc nulls last) as rn
            from {{ ref("fds_int_059_caixa_pj") }}
            where selecao_pmcmv_e = 'NOVO PMCMV-E'
        ) t where rn = 1
    ),

    trabalho_social as (
        select * from {{ ref("fds_trabalho_social") }}
    ),

    -- Desembolso acumulado: soma ABS das liberações reais (ic_credito = '0')
    desembolso_acumulado as (
        select
            right(apf, 6) as apf_raiz,
            sum(abs(vr_liberado)) as vr_total_desembolsado,
            count(*) as qt_liberacoes_total,
            max(dt_liberacao) as dt_ultima_liberacao
        from {{ ref("fds_financeiro_mensal") }}
        where ic_credito = '0'
          and vr_liberado is not null
        group by right(apf, 6)
    ),

select
    c.apf,

    -- 1. Entidade Organizadora
    c.eo_nome,
    -- CNPJ padronizado para 14 dígitos (62% da fonte vem com 13, faltando zero à esquerda)
    lpad(c.eo_cnpj, 14, '0') as eo_cnpj,
    c.co_nivel_hab_eo,
    c.ic_substituicao_eo,
    c.dt_substituicao_eo,
    c.eo_substituta_nome,
    c.eo_substituta_cnpj,

    -- 2. Dados do Empreendimento
    c.empreendimento_nome,
    -- Construtora: 100% placeholder no FDS ('NÃO INFORMADO', cnpj='0').
    -- Ao que parece no MCMV Entidades a EO faz o papel de construtora.
    c.construtora_nome,
    c.construtora_cnpj,
    c.agente_financeiro,
    -- UHs: campos construcao/projeto são mutuamente exclusivos na fonte.
    -- Quando um tem valor o outro é 0. Usar GREATEST para obter o real.
    greatest(coalesce(c.qt_uh_construcao, 0), coalesce(c.qt_uh_projeto, 0)) as quantidade_uh,
    c.qt_uh_construcao,
    c.qt_uh_projeto,
    -- Heurística: 3,3 pessoas por família
    floor(greatest(coalesce(c.qt_uh_construcao, 0), coalesce(c.qt_uh_projeto, 0)) * 3.3)::int as pessoas_atendidas,

    -- 3. Localização
    c.municipio,
    c.uf,
    c.cod_ibge,

    -- 4. Tipologia (códigos ainda pendentes de decodificação, mantendo os dois)
    c.co_tipo_edificacao,
    case c.co_tipo_edificacao
        when 1 then 'Casa'
        when 2 then 'Misto'
        when 3 then 'Apartamento'
        else 'Não Informado'
    end as tipologia,
    c.co_regime_obra,
    c.co_modalidade,

    -- 5. Valores Contratuais
    coalesce(c.vr_total_investimento, 0.0) as valor_contratado,
    coalesce(c.vr_financiamento_fds, 0.0) as valor_financiamento_fds,
    coalesce(c.vr_total_contrapartidas, 0.0) as valor_contrapartidas,
    case
        when greatest(coalesce(c.qt_uh_construcao, 0), coalesce(c.qt_uh_projeto, 0)) > 0
        then c.vr_total_investimento / greatest(coalesce(c.qt_uh_construcao, 0), coalesce(c.qt_uh_projeto, 0))
        else 0.0
    end as valor_por_uh,

    -- 6. Datas do Contrato
    -- Tratamento: data sentinel 1900-01-01 → NULL (1 caso: APF 63520415)
    nullif(c.dt_contratacao, '1900-01-01'::date) as dt_contratacao,
    nullif(c.dt_inicio_obra, '1900-01-01'::date) as dt_inicio_obra,
    c.dt_previsao_conclusao,

    -- 7. Situação (complementar da INT 059 como fonte primária de status textual)
    coalesce(i.situacao_gefus, 'Não mapeado') as situacao_gefus,
    coalesce(i.fase_contrato, 'Não mapeada') as fase_contrato,
    o.co_situacao_operacao,
    o.co_andamento_operacao,

    -- 8. Evolução Física
    coalesce(o.pct_obra_prevista, 0.0) as percentual_obra_prevista,
    coalesce(o.pct_obra_realizada, 0.0) as percentual_execucao_fisica,

    -- 9. UHs — Entregas e Status
    -- OBS: qt_uh_concluidas é NULL na fonte FDS mesmo quando há alienação.
    -- Usar qt_uh_alienadas como indicador real de entrega.
    coalesce(o.qt_uh_concluidas, 0) as qt_uh_concluidas,
    coalesce(o.qt_uh_alienadas, 0) as qt_uh_alienadas,
    coalesce(o.qt_uh_sem_habitese, 0) as qt_uh_sem_habitese,
    coalesce(o.qt_uh_construcao_parcial, 0) as qt_uh_construcao_parcial,
    coalesce(o.qt_uh_ocupacao_irregular, 0) as qt_uh_ocupacao_irregular,

    -- 10. Taxa de entrega (UHs alienadas / UHs contratadas)
    case
        when greatest(coalesce(c.qt_uh_construcao, 0), coalesce(c.qt_uh_projeto, 0)) > 0
        then round(
            coalesce(o.qt_uh_alienadas, 0)::numeric
            / greatest(coalesce(c.qt_uh_construcao, 0), coalesce(c.qt_uh_projeto, 0)) * 100,
            2
        )
        else 0.0
    end as pct_entrega,

    -- 11. Paralisação e Alertas
    o.dt_paralisacao,
    o.co_classificacao_paralisado,
    o.ic_invadido,
    o.dt_invasao,

    -- 12. Datas de marcos
    o.dt_conclusao_obra,
    o.dt_entrega,
    o.dt_previsao_entrega,

    -- 13. Evolução Financeira (acumulado)
    coalesce(d.vr_total_desembolsado, 0.0) as valor_desembolsado,
    coalesce(d.qt_liberacoes_total, 0) as qt_liberacoes,
    d.dt_ultima_liberacao,
    case
        when coalesce(c.vr_total_investimento, 0.0) > 0
        then round((coalesce(d.vr_total_desembolsado, 0.0) / c.vr_total_investimento) * 100, 2)
        else 0.0
    end as percentual_execucao_financeira,

    -- 14. Divergência Físico-Financeira (gap em pontos percentuais)
    case
        when coalesce(c.vr_total_investimento, 0.0) > 0
        then round(
            (coalesce(d.vr_total_desembolsado, 0.0) / c.vr_total_investimento) * 100
            - coalesce(o.pct_obra_realizada, 0.0),
            2
        )
        else 0.0
    end as divergencia_fisico_financeira,

    -- 15. Trabalho Social
    ts.co_situacao_trabalho_social,
    ts.pct_execucao_ts,
    ts.dt_aprovacao_pts,

    -- 16. Tempo entre contratação e início da obra (dias)
    -- Usa as datas já tratadas (sentinel 1900 → NULL)
    case
        when nullif(c.dt_inicio_obra, '1900-01-01'::date) is not null
         and nullif(c.dt_contratacao, '1900-01-01'::date) is not null
        then (c.dt_inicio_obra - c.dt_contratacao)
    end as dias_contratacao_inicio,

    -- 17. Coordenadas GPS (DMS)
    -- Tratamento: GPS (0,0,0) → NULL (32 casos = 9%).
    -- Evita pontos em locais aleatórios do mapa
    nullif(c.gps_lat_grau, 0) as gps_lat_grau,
    case when c.gps_lat_grau != 0 then c.gps_lat_minuto end as gps_lat_minuto,
    case when c.gps_lat_grau != 0 then c.gps_lat_segundo end as gps_lat_segundo,
    nullif(c.gps_long_grau, 0) as gps_long_grau,
    case when c.gps_long_grau != 0 then c.gps_long_minuto end as gps_long_minuto,
    case when c.gps_long_grau != 0 then c.gps_long_segundo end as gps_long_segundo

from cadastro c
left join obra o on c.apf = o.apf
left join int059 i on c.apf = i.apf
left join trabalho_social ts on c.apf = ts.apf
left join desembolso_acumulado d on left(c.apf, 6) = d.apf_raiz
