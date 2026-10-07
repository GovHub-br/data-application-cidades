{{ config(materialized="table") }}

-- Gold: Por que as propostas do FNHIS Sub-50 ficaram de fora. Grão: proposta × motivo.
--
-- Propostas NÃO ENQUADRADAS têm a justificativa em texto livre, às vezes com vários motivos
-- colados ("Ente apresentou proposta retificadora.Titularidade de áreas..."). A categorização é
-- por regra; uma proposta pode ter mais de um motivo (uma linha por motivo). As que passaram no
-- enquadramento mas não foram selecionadas entram com o motivo da seleção (cota insuficiente da
-- UF ou município já contemplado), para que a tabela responda "por que ficou de fora" inteira.

with
    prop as (
        select * from {{ ref("prata_fnhis_propostas") }}
        where not ic_selecionada
    ),

    regras as (
        select * from (values
            (1, 'Titularidade da área em desacordo', 'titularidade'),
            (2, 'Proposta retificadora apresentada', 'retificadora'),
            (3, 'UH acima do limite para o porte do município', 'maior que o limite|limite disposto'),
            (4, 'Área suscetível a risco / declaração de risco', 'risco|deslizamento|inunda'),
            (5, 'Excede nº de propostas por município/proponente', 'supera n|n.mero de propostas'),
            (6, 'Município acima de 50 mil habitantes', 'popula..o|mil hab'),
            (7, 'Documentação ausente ou irregular', 'document|of.cio|assinatura|certid')
        ) as r (ordem, motivo, padrao)
    ),

    nao_enq as (
        select p.numero_proposta, r.ordem, r.motivo
        from prop p
        join regras r on p.justificativa_nao_enquadramento ~* r.padrao
        where p.resultado_selecao = 'Não enquadrada'
    ),

    nao_enq_outros as (
        select p.numero_proposta, 99 as ordem, 'Outros (ver justificativa)' as motivo
        from prop p
        where p.resultado_selecao = 'Não enquadrada'
          and not exists (select 1 from nao_enq n where n.numero_proposta = p.numero_proposta)
    ),

    fora_selecao as (
        select numero_proposta, 50 as ordem, resultado_selecao as motivo
        from prop
        where resultado_selecao in ('Cota insuficiente da UF', 'Município já contemplado', 'Outra')
    ),

    motivos as (
        select * from nao_enq
        union all select * from nao_enq_outros
        union all select * from fora_selecao
    )

select
    p.numero_proposta,
    p.uf,
    {{ regiao_da_uf("p.uf") }} as regiao,
    p.municipio,
    p.cod_ibge,
    p.proponente_esfera,
    p.quantidade_uh,
    p.resultado_selecao,
    case when p.resultado_selecao = 'Não enquadrada' then 'Enquadramento' else 'Seleção' end as etapa_em_que_saiu,
    m.motivo,
    m.ordem as ordem_motivo,
    count(*) over (partition by p.numero_proposta) as qt_motivos_da_proposta,
    p.justificativa_nao_enquadramento
from motivos m
join prop p on p.numero_proposta = m.numero_proposta
