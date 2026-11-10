{{ config(materialized="table") }}

-- Ouro: contratos do FAR agregados pelo perfil do titular no CadÚnico (raça/cor,
-- situação de rua, PCD, Bolsa Família) e gastos da família com energia e água.
-- Agregada e sem contrato nem município, porque raça/cor é dado sensível.
-- Médias de gasto = soma / quantidade com o valor informado.
with
    contrato as (
        select
            c.geracao,
            c.agente_financeiro,
            c.uf,
            c.faixa_renda,
            c.ic_ativo,
            c.ic_inadimplente,
            cad.chave_contrato is not null as ic_encontrado_cadunico,
            cad.raca_cor,
            cad.ic_situacao_rua,
            cad.tempo_situacao_rua,
            cad.ic_pcd,
            cad.ic_bolsa_familia,
            case
                when cad.dt_atualizacao_cadastro is null or c.dt_assinatura is null then null
                when cad.dt_atualizacao_cadastro >= c.dt_assinatura then 'Depois da assinatura'
                else 'Antes da assinatura'
            end as momento_cadastro,
            cad.cod_ibge_cadastro = c.cod_ibge as ic_mesmo_municipio,
            cad.despesa_energia,
            cad.despesa_agua_esgoto
        from {{ ref("prata_far_contrato_pf") }} c
        left join {{ ref("prata_far_beneficiario_cadunico") }} cad
            on cad.geracao = c.geracao
            and cad.agente_financeiro = c.agente_financeiro
            and cad.nu_contrato = c.nu_contrato
    )

select
    geracao,
    agente_financeiro,
    uf,
    faixa_renda,
    ic_encontrado_cadunico,
    raca_cor,
    ic_situacao_rua,
    tempo_situacao_rua,
    ic_pcd,
    ic_bolsa_familia,
    momento_cadastro,
    ic_mesmo_municipio,
    count(*) as qt_contratos,
    count(*) filter (where ic_ativo) as qt_ativos,
    count(*) filter (where ic_inadimplente) as qt_inadimplentes,
    count(despesa_energia) as qt_com_despesa_energia,
    sum(despesa_energia) as soma_despesa_energia,
    count(despesa_agua_esgoto) as qt_com_despesa_agua,
    sum(despesa_agua_esgoto) as soma_despesa_agua
from contrato
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
