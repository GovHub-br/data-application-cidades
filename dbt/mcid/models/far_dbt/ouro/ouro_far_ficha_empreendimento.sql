{{ config(materialized="table") }}

-- Ouro: Ficha do Empreendimento — uma linha por APF para o dashboard
-- Aplica as regras de negócio finais (status, prazo, ritmo) sobre a silver.
with base_silver as (select * from {{ ref("prata_far_empreendimento") }}),
    dominio as (select campo, codigo, rotulo from {{ ref("dominio_far_perguntas") }})

select
    -- Identificadores
    apf,
    portaria_selecao,

    -- Nomes formatados
    upper(empreendimento_nome) as nome_empreendimento,
    proponente_cnpj,
    upper(proponente_nome) as nome_proponente,
    tomador_cnpj,
    upper(tomador_nome) as nome_tomador,
    agente_financeiro,

    -- Escopo e Tipologia
    quantidade_uh,
    pessoas_atendidas,
    tipologia,

    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,

    -- Chave de busca no dashboard: APF - Município/UF - Nome
    concat(
        apf, ' - ', municipio, '/', uf, ' - ', upper(empreendimento_nome)
    ) as apf_municipio_empreendimento,

    -- Originação e Demanda: domínio do catálogo do HIS_MCIDADES_CONSOLIDADO (seed dominio_far_perguntas).
    upper(coalesce(og.rotulo, 'Outro (' || coalesce(co_originacao::text, 'N/A') || ')')) as metodo,
    upper(coalesce(td.rotulo, 'Não identificado (' || coalesce(co_tipo_demanda::text, 'N/A') || ')')) as demanda,

    -- Status do Projeto
    situacao_empreendimento,
    case
        when percentual_execucao_fisica = 100
        then 'Concluído'
        when percentual_execucao_fisica = 0
        then 'Não Iniciado'
        when percentual_execucao_fisica > 0 and percentual_execucao_fisica < 100
        then 'Em Andamento'
        else 'Status Desconhecido'
    end as status_execucao_simplificado,

    -- Valores Contratuais e KPIs
    valor_contratado,
    valor_aporte_adicional,
    valor_por_uh,
    dt_contratacao,

    -- Evolução e Entregas
    percentual_execucao_fisica,
    dt_previsao_entrega,
    dt_conclusao_obra,
    dt_entrega,

    -- Regra de negócio: atraso na entrega
    case
        when situacao_empreendimento != 'ENTREGUE' and dt_previsao_entrega < current_date
        then 'Em Atraso'
        when situacao_empreendimento = 'ENTREGUE'
        then 'Entregue'
        else 'Dentro do Prazo'
    end as status_prazo,

    -- Evolução Financeira
    valor_desembolsado,
    percentual_execucao_financeira,

    -- Regra de negócio: aderência do desembolso à execução física (margem de 5%)
    case
        when percentual_execucao_financeira > (percentual_execucao_fisica + 5)
        then 'Desembolso Adiantado'
        when percentual_execucao_financeira < (percentual_execucao_fisica - 5)
        then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro

from base_silver
left join dominio og on og.campo = 'originacao' and og.codigo = lpad(base_silver.co_originacao::text, 3, '0')
left join dominio td on td.campo = 'tipo_demanda' and td.codigo = lpad(base_silver.co_tipo_demanda::text, 3, '0')
