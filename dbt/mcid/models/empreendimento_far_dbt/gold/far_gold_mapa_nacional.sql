{{ config(materialized="table", alias="gold_mapa_nacional") }}

-- Gold: Mapa Nacional — agregação geográfica para o Superset
-- Duas visões empilhadas, discriminadas por `nivel`: uma por UF (com código ISO
-- para o Country Map) e uma por macrorregião. A tabela IBGE traz nome e região.
with
    fichas as (select * from {{ ref("far_gold_ficha_empreendimento") }}),

    -- distinct: a api_ibge_uf tem cada UF duplicada (54 linhas p/ 27 siglas) e o join
    -- dobraria as somas
    ibge_uf as (
        select distinct sigla, nome as estado_nome, regiao_sigla, regiao_nome
        from {{ source("raw", "api_ibge_uf") }}
    ),

    agg_uf as (
        select
            f.uf,
            i.estado_nome,
            i.regiao_sigla,
            i.regiao_nome,
            -- Código ISO 3166-2 exigido pelo Country Map do Superset
            'BR-' || f.uf as iso_3166_2,
            count(distinct f.municipio_uf) as total_municipios,
            count(f.apf) as total_empreendimentos,
            coalesce(sum(f.quantidade_uh), 0) as total_uhs,
            coalesce(sum(f.valor_contratado), 0.0) as total_valor_contratado,
            coalesce(sum(f.valor_desembolsado), 0.0) as total_valor_desembolsado,
            coalesce(avg(f.percentual_execucao_fisica), 0.0) as media_execucao_fisica
        from fichas f
        left join ibge_uf i on f.uf = i.sigla
        group by f.uf, i.estado_nome, i.regiao_sigla, i.regiao_nome
    )

-- Nível UF: uma linha por estado
select
    uf,
    estado_nome,
    iso_3166_2,
    regiao_sigla,
    regiao_nome,
    'uf' as nivel,
    total_municipios,
    total_empreendimentos,
    total_uhs,
    total_valor_contratado,
    total_valor_desembolsado,
    round(media_execucao_fisica, 1) as media_execucao_fisica
from agg_uf

union all

-- Nível região: agrega os estados por macrorregião
select
    regiao_sigla as uf,
    regiao_nome as estado_nome,
    null as iso_3166_2,
    regiao_sigla,
    regiao_nome,
    'regiao' as nivel,
    sum(total_municipios) as total_municipios,
    sum(total_empreendimentos) as total_empreendimentos,
    sum(total_uhs) as total_uhs,
    sum(total_valor_contratado) as total_valor_contratado,
    sum(total_valor_desembolsado) as total_valor_desembolsado,
    round(avg(media_execucao_fisica), 1) as media_execucao_fisica
from agg_uf
group by regiao_sigla, regiao_nome
