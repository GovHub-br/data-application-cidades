{{ config(materialized="table") }}

-- Gold agregada para Superset: cards, rankings e mapas de gargalo/desempenho.
with
    base as (select * from {{ ref("indicadores_gargalo_desempenho") }}),

    agregada as (
        select
            'nacional'::text as nivel_agregacao,
            'Todas'::text as frente,
            null::text as uf,
            null::text as municipio,
            null::text as responsavel_tipo,
            null::text as responsavel_id,
            null::text as responsavel_nome,
            count(*) as total_empreendimentos,
            sum(quantidade_uh) as total_uh,
            sum(valor_contratado) as valor_contratado,
            sum(valor_desembolsado) as valor_desembolsado,
            sum(saldo_contratado_a_desembolsar) as saldo_contratado_a_desembolsar,
            round(avg(percentual_execucao_fisica), 2) as media_execucao_fisica,
            round(avg(percentual_execucao_financeira), 2) as media_execucao_financeira,
            round(avg(score_gargalo), 2) as media_score_gargalo,
            round(avg(nullif(dias_atraso, 0)), 2) as atraso_medio_dias,
            round(avg(dias_sem_atualizacao), 2) as media_dias_sem_atualizacao,
            count(*) filter (where flag_atraso) as qtd_obras_atrasadas,
            count(*) filter (where flag_paralisacao) as qtd_obras_paralisadas,
            count(*) filter (
                where flag_sem_atualizacao_recente
            ) as qtd_sem_atualizacao_recente,
            count(*) filter (
                where flag_baixa_execucao_fisica
            ) as qtd_baixa_execucao_fisica,
            count(*) filter (
                where flag_baixa_execucao_financeira
            ) as qtd_baixa_execucao_financeira,
            count(*) filter (where flag_gargalo_financeiro) as qtd_gargalo_financeiro,
            count(*) filter (
                where flag_contrato_sem_evolucao
            ) as qtd_contrato_sem_evolucao,
            count(*) filter (where flag_entrega_em_risco) as qtd_entregas_em_risco,
            count(*) filter (
                where classificacao_gargalo = 'Crítico'
            ) as qtd_casos_criticos
        from base

        union all

        select
            'frente'::text as nivel_agregacao,
            frente,
            null::text as uf,
            null::text as municipio,
            null::text as responsavel_tipo,
            null::text as responsavel_id,
            null::text as responsavel_nome,
            count(*) as total_empreendimentos,
            sum(quantidade_uh) as total_uh,
            sum(valor_contratado) as valor_contratado,
            sum(valor_desembolsado) as valor_desembolsado,
            sum(saldo_contratado_a_desembolsar) as saldo_contratado_a_desembolsar,
            round(avg(percentual_execucao_fisica), 2) as media_execucao_fisica,
            round(avg(percentual_execucao_financeira), 2) as media_execucao_financeira,
            round(avg(score_gargalo), 2) as media_score_gargalo,
            round(avg(nullif(dias_atraso, 0)), 2) as atraso_medio_dias,
            round(avg(dias_sem_atualizacao), 2) as media_dias_sem_atualizacao,
            count(*) filter (where flag_atraso) as qtd_obras_atrasadas,
            count(*) filter (where flag_paralisacao) as qtd_obras_paralisadas,
            count(*) filter (
                where flag_sem_atualizacao_recente
            ) as qtd_sem_atualizacao_recente,
            count(*) filter (
                where flag_baixa_execucao_fisica
            ) as qtd_baixa_execucao_fisica,
            count(*) filter (
                where flag_baixa_execucao_financeira
            ) as qtd_baixa_execucao_financeira,
            count(*) filter (where flag_gargalo_financeiro) as qtd_gargalo_financeiro,
            count(*) filter (
                where flag_contrato_sem_evolucao
            ) as qtd_contrato_sem_evolucao,
            count(*) filter (where flag_entrega_em_risco) as qtd_entregas_em_risco,
            count(*) filter (
                where classificacao_gargalo = 'Crítico'
            ) as qtd_casos_criticos
        from base
        group by frente

        union all

        select
            'uf'::text as nivel_agregacao,
            frente,
            uf,
            null::text as municipio,
            null::text as responsavel_tipo,
            null::text as responsavel_id,
            null::text as responsavel_nome,
            count(*) as total_empreendimentos,
            sum(quantidade_uh) as total_uh,
            sum(valor_contratado) as valor_contratado,
            sum(valor_desembolsado) as valor_desembolsado,
            sum(saldo_contratado_a_desembolsar) as saldo_contratado_a_desembolsar,
            round(avg(percentual_execucao_fisica), 2) as media_execucao_fisica,
            round(avg(percentual_execucao_financeira), 2) as media_execucao_financeira,
            round(avg(score_gargalo), 2) as media_score_gargalo,
            round(avg(nullif(dias_atraso, 0)), 2) as atraso_medio_dias,
            round(avg(dias_sem_atualizacao), 2) as media_dias_sem_atualizacao,
            count(*) filter (where flag_atraso) as qtd_obras_atrasadas,
            count(*) filter (where flag_paralisacao) as qtd_obras_paralisadas,
            count(*) filter (
                where flag_sem_atualizacao_recente
            ) as qtd_sem_atualizacao_recente,
            count(*) filter (
                where flag_baixa_execucao_fisica
            ) as qtd_baixa_execucao_fisica,
            count(*) filter (
                where flag_baixa_execucao_financeira
            ) as qtd_baixa_execucao_financeira,
            count(*) filter (where flag_gargalo_financeiro) as qtd_gargalo_financeiro,
            count(*) filter (
                where flag_contrato_sem_evolucao
            ) as qtd_contrato_sem_evolucao,
            count(*) filter (where flag_entrega_em_risco) as qtd_entregas_em_risco,
            count(*) filter (
                where classificacao_gargalo = 'Crítico'
            ) as qtd_casos_criticos
        from base
        group by frente, uf

        union all

        select
            'municipio'::text as nivel_agregacao,
            frente,
            uf,
            municipio,
            null::text as responsavel_tipo,
            null::text as responsavel_id,
            null::text as responsavel_nome,
            count(*) as total_empreendimentos,
            sum(quantidade_uh) as total_uh,
            sum(valor_contratado) as valor_contratado,
            sum(valor_desembolsado) as valor_desembolsado,
            sum(saldo_contratado_a_desembolsar) as saldo_contratado_a_desembolsar,
            round(avg(percentual_execucao_fisica), 2) as media_execucao_fisica,
            round(avg(percentual_execucao_financeira), 2) as media_execucao_financeira,
            round(avg(score_gargalo), 2) as media_score_gargalo,
            round(avg(nullif(dias_atraso, 0)), 2) as atraso_medio_dias,
            round(avg(dias_sem_atualizacao), 2) as media_dias_sem_atualizacao,
            count(*) filter (where flag_atraso) as qtd_obras_atrasadas,
            count(*) filter (where flag_paralisacao) as qtd_obras_paralisadas,
            count(*) filter (
                where flag_sem_atualizacao_recente
            ) as qtd_sem_atualizacao_recente,
            count(*) filter (
                where flag_baixa_execucao_fisica
            ) as qtd_baixa_execucao_fisica,
            count(*) filter (
                where flag_baixa_execucao_financeira
            ) as qtd_baixa_execucao_financeira,
            count(*) filter (where flag_gargalo_financeiro) as qtd_gargalo_financeiro,
            count(*) filter (
                where flag_contrato_sem_evolucao
            ) as qtd_contrato_sem_evolucao,
            count(*) filter (where flag_entrega_em_risco) as qtd_entregas_em_risco,
            count(*) filter (
                where classificacao_gargalo = 'Crítico'
            ) as qtd_casos_criticos
        from base
        group by frente, uf, municipio

        union all

        select
            'responsavel'::text as nivel_agregacao,
            frente,
            uf,
            null::text as municipio,
            responsavel_tipo,
            responsavel_id,
            responsavel_nome,
            count(*) as total_empreendimentos,
            sum(quantidade_uh) as total_uh,
            sum(valor_contratado) as valor_contratado,
            sum(valor_desembolsado) as valor_desembolsado,
            sum(saldo_contratado_a_desembolsar) as saldo_contratado_a_desembolsar,
            round(avg(percentual_execucao_fisica), 2) as media_execucao_fisica,
            round(avg(percentual_execucao_financeira), 2) as media_execucao_financeira,
            round(avg(score_gargalo), 2) as media_score_gargalo,
            round(avg(nullif(dias_atraso, 0)), 2) as atraso_medio_dias,
            round(avg(dias_sem_atualizacao), 2) as media_dias_sem_atualizacao,
            count(*) filter (where flag_atraso) as qtd_obras_atrasadas,
            count(*) filter (where flag_paralisacao) as qtd_obras_paralisadas,
            count(*) filter (
                where flag_sem_atualizacao_recente
            ) as qtd_sem_atualizacao_recente,
            count(*) filter (
                where flag_baixa_execucao_fisica
            ) as qtd_baixa_execucao_fisica,
            count(*) filter (
                where flag_baixa_execucao_financeira
            ) as qtd_baixa_execucao_financeira,
            count(*) filter (where flag_gargalo_financeiro) as qtd_gargalo_financeiro,
            count(*) filter (
                where flag_contrato_sem_evolucao
            ) as qtd_contrato_sem_evolucao,
            count(*) filter (where flag_entrega_em_risco) as qtd_entregas_em_risco,
            count(*) filter (
                where classificacao_gargalo = 'Crítico'
            ) as qtd_casos_criticos
        from base
        group by frente, uf, responsavel_tipo, responsavel_id, responsavel_nome
    )

select
    nivel_agregacao,
    frente,
    uf,
    municipio,
    responsavel_tipo,
    responsavel_id,
    responsavel_nome,
    total_empreendimentos,
    total_uh,
    valor_contratado,
    valor_desembolsado,
    saldo_contratado_a_desembolsar,
    media_execucao_fisica,
    media_execucao_financeira,
    media_score_gargalo,
    atraso_medio_dias,
    media_dias_sem_atualizacao,
    qtd_obras_atrasadas,
    qtd_obras_paralisadas,
    qtd_sem_atualizacao_recente,
    qtd_baixa_execucao_fisica,
    qtd_baixa_execucao_financeira,
    qtd_gargalo_financeiro,
    qtd_contrato_sem_evolucao,
    qtd_entregas_em_risco,
    qtd_casos_criticos,
    round(
        qtd_entregas_em_risco::numeric / nullif(total_empreendimentos, 0) * 100, 2
    ) as percentual_entregas_em_risco,
    round(
        qtd_casos_criticos::numeric / nullif(total_empreendimentos, 0) * 100, 2
    ) as percentual_casos_criticos,
    current_date as dt_calculo
from agregada
