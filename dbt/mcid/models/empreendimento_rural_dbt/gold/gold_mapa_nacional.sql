{{ config(materialized="table") }}

-- Gold: Mapa Nacional (Rural)
-- Produz duas visões complementares: por UF (para mapas) e por Região (para gráficos)

with
    fichas as (
        select * from {{ ref("gold_ficha_empreendimento") }}
    ),

    -- Referência IBGE: sigla, nome do estado, região.
    -- distinct: a api_ibge_uf tem cada UF duplicada (54 linhas p/ 27 siglas) e o join
    -- dobraria as somas
    ibge_uf as (
        select distinct sigla, nome as estado_nome, regiao_sigla, regiao_nome
        from {{ source("raw", "api_ibge_uf") }}
    ),

    -- Agregação por UF
    agg_uf as (
        select
            f.uf,
            i.estado_nome,
            i.regiao_sigla,
            i.regiao_nome,
            'BR-' || f.uf                         as iso_3166_2,
            count(distinct f.municipio_uf)         as total_municipios,
            count(f.apf)                           as total_empreendimentos,
            sum(f.quantidade_uh)                   as total_uhs,
            sum(f.valor_contratado)                as total_valor_contratado,
            sum(f.valor_desembolsado)              as total_valor_desembolsado,
            -- Sem `coalesce(..., 0.00)`: UF onde nenhum empreendimento tem medição física
            -- não está com 0% de execução — está sem informação. O zero pintava o mapa.
            avg(f.percentual_execucao_fisica)      as media_execucao_fisica,
            -- Média simples trata um empreendimento de 8 UH como um de 500. A ponderada
            -- por UH responde "que fração da obra do estado está feita", que é a pergunta
            -- de gestão. As duas ficam expostas, porque medem coisas diferentes.
            case
                when sum(
                    case when f.percentual_execucao_fisica is not null
                         then f.quantidade_uh end
                ) > 0
                then sum(f.percentual_execucao_fisica * f.quantidade_uh)
                     / sum(
                         case when f.percentual_execucao_fisica is not null
                              then f.quantidade_uh end
                     )
            end as media_execucao_fisica_ponderada_uh,
            count(f.percentual_execucao_fisica)    as empreendimentos_com_medicao_fisica
        from fichas f
        left join ibge_uf i on f.uf = i.sigla
        group by f.uf, i.estado_nome, i.regiao_sigla, i.regiao_nome
    ),

    -- A linha de região é agregada da BASE, não das linhas de UF.
    --
    -- Antes era `avg(media_execucao_fisica)` sobre agg_uf: média de médias. Roraima com 3
    -- empreendimentos pesava igual à Bahia com 3.000, e o número da região não era a
    -- execução da região — era a média das médias dos estados dela.
    agg_regiao as (
        select
            i.regiao_sigla,
            i.regiao_nome,
            count(distinct f.municipio_uf)    as total_municipios,
            count(f.apf)                      as total_empreendimentos,
            sum(f.quantidade_uh)              as total_uhs,
            sum(f.valor_contratado)           as total_valor_contratado,
            sum(f.valor_desembolsado)         as total_valor_desembolsado,
            avg(f.percentual_execucao_fisica) as media_execucao_fisica,
            case
                when sum(
                    case when f.percentual_execucao_fisica is not null
                         then f.quantidade_uh end
                ) > 0
                then sum(f.percentual_execucao_fisica * f.quantidade_uh)
                     / sum(
                         case when f.percentual_execucao_fisica is not null
                              then f.quantidade_uh end
                     )
            end as media_execucao_fisica_ponderada_uh,
            count(f.percentual_execucao_fisica) as empreendimentos_com_medicao_fisica
        from fichas f
        left join ibge_uf i on f.uf = i.sigla
        group by i.regiao_sigla, i.regiao_nome
    )

select
    uf,
    estado_nome,
    iso_3166_2,
    regiao_sigla,
    regiao_nome,
    'uf'                    as nivel,
    total_municipios,
    total_empreendimentos,
    total_uhs,
    total_valor_contratado,
    total_valor_desembolsado,
    round(media_execucao_fisica, 1) as media_execucao_fisica,
    round(media_execucao_fisica_ponderada_uh, 1) as media_execucao_fisica_ponderada_uh,
    empreendimentos_com_medicao_fisica
from agg_uf

union all

select
    regiao_sigla            as uf,
    regiao_nome             as estado_nome,
    null                    as iso_3166_2,
    regiao_sigla,
    regiao_nome,
    'regiao'                as nivel,
    total_municipios,
    total_empreendimentos,
    total_uhs,
    total_valor_contratado,
    total_valor_desembolsado,
    round(media_execucao_fisica, 1) as media_execucao_fisica,
    round(media_execucao_fisica_ponderada_uh, 1) as media_execucao_fisica_ponderada_uh,
    empreendimentos_com_medicao_fisica
from agg_regiao
