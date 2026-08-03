{{ config(materialized="table") }}

-- Gold: indicadores de gargalo e desempenho MCMV.
-- Uma linha por empreendimento/APF, unificando FAR e FDS para alimentar alertas
-- estratégicos, rankings de risco e filtros de dashboard.

with
    far_ultima_financeira as (
        select
            apf,
            max(mes::date) as dt_ultima_liberacao,
            sum(vr_liberado_mes) as valor_liberado_historico
        from {{ ref("evolucao_financeira") }}
        group by apf
    ),

    far_ultima_fisica as (
        select
            apf,
            max(
                case
                    when mes ~ '^\d{4}-\d{2}-\d{2}$' then mes::date
                    when mes ~ '^\d{4}-\d{2}$' then to_date(mes, 'YYYY-MM')
                end
            ) as dt_ultima_medicao_fisica
        from {{ ref("execucao_fisica_financeira_chart") }}
        group by apf
    ),

    fds_ultima_financeira as (
        select
            apf,
            max(
                case
                    when mes ~ '^\d{4}-\d{2}-\d{2}$' then mes::date
                    when mes ~ '^\d{4}-\d{2}$' then to_date(mes, 'YYYY-MM')
                end
            ) as dt_ultima_liberacao,
            sum(valor_liberado_mensal) as valor_liberado_historico
        from {{ ref("fds_evolucao_financeira_chart") }}
        group by apf
    ),

    far as (
        select
            'FAR'::text as frente,
            f.apf,
            f.nome_empreendimento,
            f.municipio,
            f.uf,
            f.municipio_uf,
            f.agente_financeiro,
            'Tomador/Proponente'::text as responsavel_tipo,
            coalesce(nullif(f.tomador_cnpj, ''), nullif(f.proponente_cnpj, '')) as responsavel_id,
            coalesce(nullif(f.nome_tomador, ''), nullif(f.nome_proponente, 'Não Informado'), f.nome_proponente) as responsavel_nome,
            f.situacao_empreendimento as situacao_operacional,
            f.status_prazo,
            f.status_execucao_simplificado as status_execucao,
            f.ritmo_fisico_financeiro as status_ritmo,
            f.quantidade_uh,
            f.valor_contratado,
            f.valor_desembolsado,
            coalesce(ff.valor_liberado_historico, f.valor_desembolsado) as valor_liberado_historico,
            greatest(coalesce(f.valor_contratado, 0) - coalesce(f.valor_desembolsado, 0), 0) as saldo_contratado_a_desembolsar,
            f.percentual_execucao_fisica,
            null::numeric as percentual_obra_prevista,
            f.percentual_execucao_financeira,
            round(coalesce(f.percentual_execucao_financeira, 0) - coalesce(f.percentual_execucao_fisica, 0), 2) as gap_fisico_financeiro_pp,
            f.dt_contratacao,
            null::date as dt_inicio_obra,
            f.dt_previsao_entrega as dt_previsao_conclusao,
            f.dt_conclusao_obra,
            f.dt_entrega,
            null::date as dt_paralisacao,
            ff.dt_ultima_liberacao,
            fu.dt_ultima_medicao_fisica,
            nullif(
                greatest(
                    coalesce(ff.dt_ultima_liberacao, '1900-01-01'::date),
                    coalesce(fu.dt_ultima_medicao_fisica, '1900-01-01'::date),
                    coalesce(f.dt_entrega, '1900-01-01'::date),
                    coalesce(f.dt_conclusao_obra, '1900-01-01'::date),
                    coalesce(f.dt_contratacao, '1900-01-01'::date)
                ),
                '1900-01-01'::date
            ) as dt_ultima_atualizacao
        from {{ ref("ficha_empreendimento") }} f
        left join far_ultima_financeira ff on f.apf = ff.apf
        left join far_ultima_fisica fu on f.apf = fu.apf
    ),

    fds as (
        select
            'FDS'::text as frente,
            f.apf,
            f.nome_empreendimento,
            f.municipio,
            f.uf,
            f.municipio_uf,
            f.agente_financeiro,
            'Entidade Organizadora'::text as responsavel_tipo,
            f.cnpj_eo as responsavel_id,
            f.nome_eo as responsavel_nome,
            f.situacao_gefus as situacao_operacional,
            case
                when coalesce(f.status_entrega, '') = 'Totalmente Entregue' then 'Entregue'
                when coalesce(e.dt_previsao_entrega, f.dt_previsao_conclusao) < current_date
                    and coalesce(f.percentual_execucao_fisica, 0) < 100
                    then 'Em Atraso'
                else 'Dentro do Prazo'
            end as status_prazo,
            f.semaforo_alerta as status_execucao,
            f.status_ritmo_obra as status_ritmo,
            f.quantidade_uh,
            f.valor_contratado,
            f.valor_desembolsado,
            coalesce(fu.valor_liberado_historico, f.valor_desembolsado) as valor_liberado_historico,
            greatest(coalesce(f.valor_contratado, 0) - coalesce(f.valor_desembolsado, 0), 0) as saldo_contratado_a_desembolsar,
            f.percentual_execucao_fisica,
            f.percentual_obra_prevista,
            f.percentual_execucao_financeira,
            f.divergencia_fisico_financeira as gap_fisico_financeiro_pp,
            f.dt_contratacao,
            f.dt_inicio_obra,
            coalesce(e.dt_previsao_entrega, f.dt_previsao_conclusao) as dt_previsao_conclusao,
            f.dt_conclusao_obra,
            f.dt_entrega,
            e.dt_paralisacao,
            coalesce(e.dt_ultima_liberacao, fu.dt_ultima_liberacao) as dt_ultima_liberacao,
            null::date as dt_ultima_medicao_fisica,
            nullif(
                greatest(
                    coalesce(e.dt_ultima_liberacao, fu.dt_ultima_liberacao, '1900-01-01'::date),
                    coalesce(f.dt_entrega, '1900-01-01'::date),
                    coalesce(f.dt_conclusao_obra, '1900-01-01'::date),
                    coalesce(f.dt_inicio_obra, '1900-01-01'::date),
                    coalesce(f.dt_contratacao, '1900-01-01'::date)
                ),
                '1900-01-01'::date
            ) as dt_ultima_atualizacao
        from {{ ref("fds_ficha_empreendimento") }} f
        left join {{ ref("fds_empreendimento") }} e on f.apf = e.apf
        left join fds_ultima_financeira fu on f.apf = fu.apf
    ),

    unificada as (
        select * from far
        union all
        select * from fds
    ),

    metricas as (
        select
            *,
            case
                when dt_ultima_atualizacao is not null then current_date - dt_ultima_atualizacao
            end as dias_sem_atualizacao,
            case
                when dt_previsao_conclusao is not null
                    and dt_previsao_conclusao < current_date
                    and coalesce(percentual_execucao_fisica, 0) < 100
                    then current_date - dt_previsao_conclusao
                else 0
            end as dias_atraso,
            case
                when dt_paralisacao is not null
                    and coalesce(percentual_execucao_fisica, 0) < 100
                    then current_date - dt_paralisacao
                else 0
            end as dias_paralisacao,
            case
                when coalesce(valor_contratado, 0) > 0
                    then round(saldo_contratado_a_desembolsar / valor_contratado * 100, 2)
                else 0
            end as percentual_saldo_a_desembolsar
        from unificada
    ),

    flags as (
        select
            *,
            (
                coalesce(status_prazo, '') = 'Em Atraso'
                or dias_atraso > 0
            ) as flag_atraso,
            (
                dt_paralisacao is not null
                or coalesce(situacao_operacional, '') ilike '%PARALIS%'
                or coalesce(status_execucao, '') ilike '%Paralis%'
            ) as flag_paralisacao,
            (
                coalesce(status_execucao, '') not in ('Concluído', 'Concluída')
                and coalesce(status_prazo, '') <> 'Entregue'
                and (
                    dt_ultima_atualizacao is null
                    or current_date - dt_ultima_atualizacao > 90
                )
            ) as flag_sem_atualizacao_recente,
            (
                coalesce(percentual_execucao_fisica, 0) < 100
                and (
                    coalesce(percentual_execucao_fisica, 0) < coalesce(percentual_obra_prevista, percentual_execucao_fisica) - 10
                    or (dt_previsao_conclusao < current_date and coalesce(percentual_execucao_fisica, 0) < 100)
                    or (dt_contratacao < current_date - 365 and coalesce(percentual_execucao_fisica, 0) < 30)
                )
            ) as flag_baixa_execucao_fisica,
            (
                coalesce(percentual_execucao_financeira, 0) < greatest(coalesce(percentual_execucao_fisica, 0) - 10, 0)
                or (dt_contratacao < current_date - 365 and coalesce(percentual_execucao_financeira, 0) < 30)
            ) as flag_baixa_execucao_financeira,
            (
                coalesce(valor_contratado, 0) > 0
                and percentual_saldo_a_desembolsar >= 30
                and coalesce(percentual_execucao_fisica, 0) < 95
            ) as flag_gargalo_financeiro,
            (
                dt_contratacao < current_date - 180
                and coalesce(percentual_execucao_fisica, 0) = 0
                and coalesce(percentual_execucao_financeira, 0) = 0
            ) as flag_contrato_sem_evolucao
        from metricas
    ),

    pontuacao as (
        select
            *,
            (
                case when flag_atraso then 2 else 0 end
                + case when flag_paralisacao then 2 else 0 end
                + case when flag_sem_atualizacao_recente then 1 else 0 end
                + case when flag_baixa_execucao_fisica then 1 else 0 end
                + case when flag_baixa_execucao_financeira then 1 else 0 end
                + case when flag_gargalo_financeiro then 1 else 0 end
                + case when flag_contrato_sem_evolucao then 1 else 0 end
            ) as score_gargalo
        from flags
    )

select
    concat(frente, ':', apf) as id_indicador,
    frente,
    apf,
    nome_empreendimento,
    municipio,
    uf,
    municipio_uf,
    agente_financeiro,
    responsavel_tipo,
    responsavel_id,
    responsavel_nome,
    situacao_operacional,
    status_prazo,
    status_execucao,
    status_ritmo,
    quantidade_uh,
    valor_contratado,
    valor_desembolsado,
    valor_liberado_historico,
    saldo_contratado_a_desembolsar,
    percentual_saldo_a_desembolsar,
    percentual_execucao_fisica,
    percentual_obra_prevista,
    percentual_execucao_financeira,
    gap_fisico_financeiro_pp,
    dt_contratacao,
    dt_inicio_obra,
    dt_previsao_conclusao,
    dt_conclusao_obra,
    dt_entrega,
    dt_paralisacao,
    dt_ultima_liberacao,
    dt_ultima_medicao_fisica,
    dt_ultima_atualizacao,
    dias_sem_atualizacao,
    dias_atraso,
    dias_paralisacao,
    flag_atraso,
    flag_paralisacao,
    flag_sem_atualizacao_recente,
    flag_baixa_execucao_fisica,
    flag_baixa_execucao_financeira,
    flag_gargalo_financeiro,
    flag_contrato_sem_evolucao,
    (
        coalesce(percentual_execucao_fisica, 0) < 100
        and (
            flag_atraso
            or flag_paralisacao
            or flag_sem_atualizacao_recente
            or flag_baixa_execucao_fisica
            or flag_baixa_execucao_financeira
        )
    ) as flag_entrega_em_risco,
    score_gargalo,
    case
        when score_gargalo >= 5 then 'Crítico'
        when score_gargalo >= 3 then 'Alto'
        when score_gargalo >= 1 then 'Médio'
        else 'Baixo'
    end as classificacao_gargalo,
    concat_ws(
        ', ',
        case when flag_atraso then 'atraso' end,
        case when flag_paralisacao then 'paralisacao' end,
        case when flag_sem_atualizacao_recente then 'sem_atualizacao_recente' end,
        case when flag_baixa_execucao_fisica then 'baixa_execucao_fisica' end,
        case when flag_baixa_execucao_financeira then 'baixa_execucao_financeira' end,
        case when flag_gargalo_financeiro then 'gargalo_financeiro' end,
        case when flag_contrato_sem_evolucao then 'contrato_sem_evolucao' end
    ) as indicadores_acionados,
    current_date as dt_calculo
from pontuacao
