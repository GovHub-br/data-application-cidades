{{ config(materialized="table") }}

-- Perfil laboral da pessoa vinculada por CPF/NIS HMAC. Este mart não descreve
-- serviços executados na reforma: isso exigiria orçamento, vistoria ou medição.
with base as (
    select
        coalesce(uf, 'Não informado') as uf,
        coalesce(municipio, 'Não informado') as municipio,
        coalesce(codigo_ibge, codigo_ibge_cadastro, 'Não informado') as codigo_ibge,
        coalesce(faixa_renda, 'Não informada') as faixa_renda,
        indicador_encontrado_cadunico,
        codigo_trabalhou_semana,
        codigo_ocupacao_principal,
        codigo_trabalho_agricultura,
        codigo_carteira_assinada,
        faixa_renda_trabalho,
        indicador_renda_construcao,
        indicador_renda_flanelinha,
        indicador_renda_carregador,
        indicador_renda_catador,
        indicador_renda_servicos_gerais,
        indicador_renda_pede,
        indicador_renda_vendas,
        indicador_renda_outro,
        dt_referencia
    from {{ ref('prata_reforma_casa_brasil_acesso') }}
), agregado as (
    select
        uf,
        municipio,
        codigo_ibge,
        faixa_renda,
        coalesce(codigo_trabalhou_semana, 'Não informado') as codigo_trabalhou_semana,
        coalesce(codigo_ocupacao_principal, 'Não informado') as codigo_ocupacao_principal,
        coalesce(faixa_renda_trabalho, 'Não informada') as faixa_renda_trabalho,
        count(*) as quantidade_contratos,
        count(*) filter (where indicador_encontrado_cadunico) as quantidade_vinculada_cadunico,
        count(*) filter (where codigo_trabalho_agricultura = '1')
            as quantidade_trabalho_agricultura,
        count(*) filter (where codigo_carteira_assinada = '1')
            as quantidade_carteira_assinada,
        count(*) filter (where indicador_renda_construcao = '1')
            as quantidade_renda_construcao,
        count(*) filter (where indicador_renda_servicos_gerais = '1')
            as quantidade_renda_servicos_gerais,
        count(*) filter (where indicador_renda_vendas = '1')
            as quantidade_renda_vendas,
        count(*) filter (where indicador_renda_catador = '1')
            as quantidade_renda_catador,
        count(*) filter (where indicador_renda_carregador = '1')
            as quantidade_renda_carregador,
        count(*) filter (where indicador_renda_flanelinha = '1')
            as quantidade_renda_flanelinha,
        count(*) filter (where indicador_renda_pede = '1')
            as quantidade_renda_pede,
        count(*) filter (where indicador_renda_outro = '1')
            as quantidade_renda_outro,
        max(dt_referencia) as dt_referencia,
        current_timestamp as dt_ouro
    from base
    group by 1, 2, 3, 4, 5, 6, 7
)
select *
from agregado
-- Proteção contra reidentificação no consumo analítico.
where quantidade_contratos >= 10
