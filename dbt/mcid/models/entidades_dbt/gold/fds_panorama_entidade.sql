{{ config(materialized="table") }}

with
    silver as (
        select * from {{ ref("fds_empreendimento") }}
    ),

    agregacao_eo as (
        select
            -- Chave Primária
            eo_cnpj as cnpj_eo,
            max(upper(eo_nome)) as nome_eo,

            -- Volumetria e Resumo
            count(apf) as total_empreendimentos,
            sum(quantidade_uh) as total_uhs_contratadas,
            sum(qt_uh_alienadas) as total_uhs_entregues,
            sum(pessoas_atendidas) as total_pessoas_atendidas,
            sum(valor_contratado) as valor_total_contratado,
            sum(valor_desembolsado) as valor_total_desembolsado,

            -- Médias Ponderadas e Percentuais
            -- Execução Física Ponderada pelo Investimento
            case
                when sum(valor_contratado) > 0
                then round((sum(percentual_execucao_fisica * valor_contratado) / sum(valor_contratado)), 2)
                else 0.0
            end as media_execucao_fisica,
            
            -- Execução Financeira Geral
            case
                when sum(valor_contratado) > 0
                then round((sum(valor_desembolsado) / sum(valor_contratado)) * 100, 2)
                else 0.0
            end as media_execucao_financeira,
            
            -- Taxa de Entrega Geral
            case
                when sum(quantidade_uh) > 0
                then round((sum(qt_uh_alienadas)::numeric / sum(quantidade_uh)) * 100, 2)
                else 0.0
            end as taxa_entrega_geral,

            -- Score de Risco (Soma de alertas)
            -- 1 ponto para cada obra atrasada (físico < previsto)
            sum(case when percentual_execucao_fisica < percentual_obra_prevista then 1 else 0 end) as qtd_obras_atrasadas,
            -- 2 pontos para cada obra paralisada
            sum(case when dt_paralisacao is not null then 1 else 0 end) as qtd_obras_paralisadas,
            -- 1 ponto para divergência grave (< -5 p.p.)
            sum(case when divergencia_fisico_financeira < -5 then 1 else 0 end) as qtd_obras_divergencia_financeira,
            
            -- Geometria e Atuação
            count(distinct uf) as qtd_estados_atuacao,
            count(distinct municipio) as qtd_municipios_atuacao

        from silver
        group by eo_cnpj
    )

select
    *,
    
    -- Score de Risco Composto
    (qtd_obras_atrasadas * 1) + 
    (qtd_obras_paralisadas * 2) + 
    (qtd_obras_divergencia_financeira * 1) as score_risco,

    -- Classificação de Risco
    case
        when ((qtd_obras_atrasadas * 1) + (qtd_obras_paralisadas * 2) + (qtd_obras_divergencia_financeira * 1)) = 0 then 'Baixo Risco'
        when ((qtd_obras_atrasadas * 1) + (qtd_obras_paralisadas * 2) + (qtd_obras_divergencia_financeira * 1)) between 1 and 3 then 'Médio Risco'
        else 'Alto Risco'
    end as classificacao_risco

from agregacao_eo
