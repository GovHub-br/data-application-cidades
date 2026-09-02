{{ config(materialized="table") }}

-- Gold: Ficha do Empreendimento (Rural)
-- Consome silver/empreendimento e injeta regras de negócio finais como status, prazos e ritmo físico-financeiro.

with
    base_silver as (
        select * from {{ ref("silver_empreendimento") }}
    ),

    -- A execução financeira era recalculada três vezes no mesmo select (uma para a coluna,
    -- duas dentro do CASE do ritmo). Uma vez só, aqui, para as três leituras não poderem
    -- divergir entre si.
    --
    -- `situacao_normalizada` existe porque a origem manda a mesma situação em duas grafias
    -- — "Concluído e Entregue" (8.549 linhas) e "CONCLUÍDO E ENTREGUE" (20) — e as regras
    -- comparavam só com a maiúscula. Resultado: 20 dos ~8.569 entregues eram reconhecidos.
    calculado as (
        select
            *,
            upper(trim(situacao_empreendimento)) as situacao_normalizada,
            case
                when coalesce(valor_contratado, 0) > 0
                then round((valor_desembolsado / valor_contratado) * 100, 2)
            end as pct_execucao_financeira
        from base_silver
    )

select
    -- Identificadores
    apf,
    agente_financeiro,
    case when ic_novo_mcmv then 'NOVO MCMV RURAL' else 'PNHR (HISTÓRICO)' end as programa,

    -- Nomes
    upper(empreendimento_nome) as nome_empreendimento,
    upper(entidade_organizadora_nome) as nome_entidade_organizadora,
    entidade_organizadora_cnpj,
    upper(construtora_nome) as nome_construtora,
    construtora_cnpj,

    -- Escopo e Tipologia
    quantidade_uh_contratadas as quantidade_uh,
    quantidade_uh_entregues,
    quantidade_uh_vigentes,
    floor(quantidade_uh_contratadas * 3.3)::int as pessoas_atendidas,
    case when ic_novo_mcmv then 'Novo MCMV Rural' else 'PNHR (Rural)' end as tipologia,

    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,
    concat(
        apf,
        ' - ',
        municipio, '/', uf,
        ' - ',
        upper(empreendimento_nome)
    ) as apf_municipio_empreendimento,

    -- Coordenadas
    latitude,
    longitude,

    -- Status do Projeto
    situacao_empreendimento,
    case
        when situacao_normalizada in ('CONCLUÍDO E ENTREGUE', 'CONCLUIDA', 'CONCLUÍDA', 'ENTREGUE')
            then 'Concluído'
        when percentual_execucao_fisica is null then 'Sem Informação'
        when percentual_execucao_fisica >= 100 then 'Concluído'
        when percentual_execucao_fisica = 0 then 'Não Iniciado'
        else 'Em Andamento'
    end as status_execucao_simplificado,

    -- Valores Contratuais
    valor_contratado,
    valor_aporte_adicional,
    case
        when coalesce(quantidade_uh_contratadas, 0) > 0
        then round((valor_contratado / quantidade_uh_contratadas), 2)
    end as valor_por_uh,
    dt_contratacao,

    -- Evolução e Prazos
    percentual_execucao_fisica,
    dt_previsao_entrega,
    dt_conclusao_obra,

    -- Regra de Negócio: Atraso na Entrega
    --
    -- 'Sem Previsão' não é um detalhe: dt_previsao_entrega é nula em 99,8% da carteira, e o
    -- `else 'Dentro do Prazo'` anterior classificava toda essa massa como em dia. A coluna
    -- afirmava pontualidade sobre empreendimentos cujo prazo ninguém informou.
    case
        when situacao_normalizada in ('CONCLUÍDO E ENTREGUE', 'CONCLUIDA', 'CONCLUÍDA', 'ENTREGUE')
            then 'Entregue'
        when dt_previsao_entrega is null then 'Sem Previsão'
        when dt_previsao_entrega < current_date then 'Em Atraso'
        else 'Dentro do Prazo'
    end as status_prazo,

    -- Evolução Financeira
    valor_desembolsado,
    pct_execucao_financeira as percentual_execucao_financeira,

    -- Regra de Negócio: Ritmo Físico-Financeiro (margem de 5%)
    -- Comparar um lado conhecido com um lado desconhecido não produz ritmo nenhum.
    case
        when pct_execucao_financeira is null or percentual_execucao_fisica is null
            then 'Sem Informação'
        when pct_execucao_financeira > percentual_execucao_fisica + 5 then 'Desembolso Adiantado'
        when pct_execucao_financeira < percentual_execucao_fisica - 5 then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro,

    -- Procedência: de onde saiu cada medição e de quando ela é. Sem isto, um número de dez
    -- meses atrás aparece no dashboard com a mesma autoridade de um de ontem.
    fonte_execucao_fisica,
    dt_referencia_execucao_fisica,
    fonte_valor_desembolsado,
    dt_referencia_valor_desembolsado,
    fonte_situacao,
    dt_referencia_consolidada,
    (current_date - dt_referencia_consolidada) as dias_desde_ultima_medicao

from calculado
