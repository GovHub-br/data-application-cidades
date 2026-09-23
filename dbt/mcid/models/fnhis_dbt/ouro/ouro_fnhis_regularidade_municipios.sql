{{ config(materialized="table") }}

-- Gold: Regularidade dos municípios no SNHIS × participação no FNHIS Sub-50
-- Grão: ente (IBGE de 7 dígitos). Cruza a situação do ente no SNHIS — condição para receber
-- recurso do FNHIS — com o que ele fez no programa: se apresentou proposta, se foi selecionado,
-- se tem termo ativo. Responde, por exemplo, quantos municípios irregulares tiveram proposta
-- selecionada, ou quantos regulares nem chegaram a propor.

with
    ente as (
        select * from {{ ref("prata_fnhis_regularidade_entes") }}
    ),

    proposta as (
        select
            cod_ibge,
            count(*) as qt_propostas,
            count(*) filter (where ic_selecionada) as qt_selecionadas,
            coalesce(sum(quantidade_uh) filter (where ic_selecionada), 0) as uh_selecionadas
        from {{ ref("prata_fnhis_propostas") }}
        group by cod_ibge
    ),

    termo as (
        select
            cod_ibge,
            count(*) filter (where ic_instrumento_ativo) as qt_termos_ativos,
            coalesce(sum(valor_repasse) filter (where ic_instrumento_ativo), 0) as valor_repasse
        from {{ ref("ouro_fnhis_ficha_termo_compromisso") }}
        group by cod_ibge
    )

select
    e.cod_ibge,
    e.ente_nome,
    e.ente_esfera,
    e.uf,
    {{ regiao_da_uf("e.uf") }} as regiao,
    e.populacao,
    e.populacao < 50000 as ic_elegivel_sub50_por_populacao,
    e.ic_regiao_metropolitana,
    e.situacao_ente,
    e.ic_ente_regular,
    e.situacao_lei_fundo,
    e.situacao_lei_conselho,
    e.situacao_termo_adesao,
    e.situacao_plano_habitacional,
    e.situacao_relatorio_gestao,
    coalesce(p.qt_propostas, 0) > 0 as ic_apresentou_proposta,
    coalesce(p.qt_propostas, 0) as qt_propostas,
    coalesce(p.qt_selecionadas, 0) as qt_selecionadas,
    coalesce(p.uh_selecionadas, 0) as uh_selecionadas,
    coalesce(t.qt_termos_ativos, 0) as qt_termos_ativos,
    coalesce(t.valor_repasse, 0) as valor_repasse,
    e.dt_referencia
from ente e
left join proposta p on p.cod_ibge = e.cod_ibge
left join termo t on t.cod_ibge = e.cod_ibge
