{{ config(materialized="table") }}

-- Gold: Panorama Estadual (Rural)
-- Consolida todos os empreendimentos de um estado em KPIs, distribuições
-- por programa, agente financeiro, situação de obra e execução física.

with
    fichas as (
        select * from {{ ref("ficha_empreendimento_rural") }}
    ),

    -- Tabela de referência IBGE para traduzir sigla UF → nome completo do estado
    ibge_uf as (
        select sigla, upper(nome) as estado
        from {{ source("raw", "api_ibge_uf") }}
    ),

    -- Seção 1: Header — Grandes números por UF
    header as (
        select
            uf,
            count(distinct municipio)            as total_municipios,
            count(apf)                           as total_empreendimentos,
            coalesce(sum(quantidade_uh), 0)       as total_uhs,
            coalesce(sum(valor_contratado), 0.00)  as total_valor_contratado,
            coalesce(sum(valor_desembolsado), 0.00) as total_valor_desembolsado,
            coalesce(sum(valor_aporte_adicional), 0.00) as total_valor_aporte_adicional,
            case
                when coalesce(sum(quantidade_uh), 0) > 0
                then sum(valor_contratado) / sum(quantidade_uh)
                else 0.00
            end as valor_medio_por_uh
        from fichas
        group by uf
    ),

    -- Seção 2: Agente Financeiro — Distribuição por Agente
    dist_agente as (
        select
            uf,
            agente_financeiro               as dimensao,
            count(apf)                      as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0)  as qtd_uhs
        from fichas
        group by uf, agente_financeiro
    ),

    -- Seção 3: Programa — Distribuição por Programa (Novo MCMV vs PNHR)
    dist_programa as (
        select
            uf,
            programa                        as dimensao,
            count(apf)                      as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0)  as qtd_uhs
        from fichas
        group by uf, programa
    ),

    -- Seção 4: Situação do Empreendimento
    skeleton_situacao (situacao, ordem) as (
        values
            ('Concluído',           1),
            ('Em Andamento',         2),
            ('Não Iniciado',         3),
            ('Status Desconhecido',  4)
    ),

    ufs as (
        select distinct uf from fichas
    ),

    grade_situacao as (
        select u.uf, s.situacao, s.ordem
        from ufs u
        cross join skeleton_situacao s
    ),

    situacao_real as (
        select
            uf,
            status_execucao_simplificado as situacao,
            count(apf)             as qtd_empreendimentos,
            sum(quantidade_uh)     as qtd_uhs
        from fichas
        group by uf, status_execucao_simplificado
    ),

    dist_situacao as (
        select
            g.uf,
            g.situacao as dimensao,
            g.ordem,
            coalesce(r.qtd_empreendimentos, 0) as qtd_empreendimentos,
            coalesce(r.qtd_uhs, 0)             as qtd_uhs
        from grade_situacao g
        left join situacao_real r
            on g.uf = r.uf and g.situacao = r.situacao
    ),

    -- Seção 5: Execução Física — Distribuição por faixa percentual
    dist_execucao_fisica as (
        select
            uf,
            case
                when percentual_execucao_fisica <= 40.00 then 'Menor que 40%'
                when percentual_execucao_fisica <= 55.00 then 'De 41% a 55%'
                when percentual_execucao_fisica <= 75.00 then 'De 56% a 75%'
                else 'Maior que 75%'
            end as faixa_execucao,
            count(apf)                       as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0)  as qtd_uhs
        from fichas
        group by uf,
            case
                when percentual_execucao_fisica <= 40.00 then 'Menor que 40%'
                when percentual_execucao_fisica <= 55.00 then 'De 41% a 55%'
                when percentual_execucao_fisica <= 75.00 then 'De 56% a 75%'
                else 'Maior que 75%'
            end
    )

-- Query final: UNION ALL de todas as seções
select
    h.uf,
    i.estado,
    'header'           as secao,
    'resumo'           as dimensao,
    0                  as ordem,
    total_municipios   as qtd_empreendimentos,
    total_uhs          as qtd_uhs,
    total_valor_contratado    as valor_1,
    total_valor_desembolsado  as valor_2,
    total_valor_aporte_adicional as valor_3,
    valor_medio_por_uh        as valor_4,
    total_empreendimentos     as total_empreendimentos_uf
from header h
left join ibge_uf i on h.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    'agente_financeiro'   as secao,
    dimensao,
    0                     as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric         as valor_1,
    null::numeric         as valor_2,
    null::numeric         as valor_3,
    null::numeric         as valor_4,
    null::int             as total_empreendimentos_uf
from dist_agente d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    'programa'            as secao,
    dimensao,
    0                     as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric, null::numeric, null::numeric, null::numeric,
    null::int
from dist_programa d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    'situacao'            as secao,
    dimensao,
    ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric, null::numeric, null::numeric, null::numeric,
    null::int
from dist_situacao d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    'execucao_fisica'     as secao,
    faixa_execucao        as dimensao,
    case faixa_execucao
        when 'Menor que 40%' then 1
        when 'De 41% a 55%'  then 2
        when 'De 56% a 75%'  then 3
        when 'Maior que 75%' then 4
    end                   as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric, null::numeric, null::numeric, null::numeric,
    null::int
from dist_execucao_fisica d
left join ibge_uf i on d.uf = i.sigla
