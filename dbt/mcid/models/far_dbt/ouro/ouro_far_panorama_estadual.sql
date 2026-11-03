{{ config(materialized="table") }}

-- Ouro: Panorama Estadual — painel agregado por UF
-- Cada seção do dashboard vira um bloco do union all, identificado por `secao`:
-- o BI filtra WHERE secao = 'X' para montar cada card.
-- Cada ficha entra na sua portaria e em 'Todas as portarias', e no seu município e em
-- 'Todos os municípios', para os filtros do dashboard não somarem recortes nem
-- contarem município duas vezes.
with
    fichas as (
        select f.*, p.portaria, m.municipio as recorte_municipio
        from {{ ref("ouro_far_ficha_empreendimento") }} f
        cross join lateral (
            values (f.portaria_selecao), ('Todas as portarias')
        ) p(portaria)
        cross join lateral (
            values (coalesce(f.municipio, 'Não informado')), ('Todos os municípios')
        ) m(municipio)
    ),

    ibge_uf as (
        select sigla, upper(nome) as estado
        from {{ ref('api_ibge_uf') }}
    ),

    -- Seção 1: header — grandes números por UF
    header as (
        select
            uf,
            portaria,
            recorte_municipio,
            count(distinct municipio) as total_municipios,
            count(apf) as total_empreendimentos,
            coalesce(sum(quantidade_uh), 0) as total_uhs,
            coalesce(sum(valor_contratado), 0.0) as total_valor_contratado,
            coalesce(sum(valor_desembolsado), 0.0) as total_valor_desembolsado,
            coalesce(sum(valor_aporte_adicional), 0.0) as total_valor_aporte_adicional,
            case
                when coalesce(sum(quantidade_uh), 0) > 0
                then sum(valor_contratado) / sum(quantidade_uh)
                else 0.0
            end as valor_medio_por_uh
        from fichas
        group by uf, portaria, recorte_municipio
    ),

    -- Seção 2: distribuição de UHs por método, portaria e demanda
    dist_metodo as (
        select
            uf,
            portaria,
            recorte_municipio,
            metodo,
            count(apf) as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0) as qtd_uhs
        from fichas
        group by uf, portaria, recorte_municipio, metodo
    ),

    dist_portaria as (
        select
            uf,
            portaria,
            recorte_municipio,
            portaria_selecao,
            count(apf) as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0) as qtd_uhs
        from fichas
        group by uf, portaria, recorte_municipio, portaria_selecao
    ),

    dist_demanda as (
        select
            uf,
            portaria,
            recorte_municipio,
            demanda,
            count(apf) as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0) as qtd_uhs
        from fichas
        group by uf, portaria, recorte_municipio, demanda
    ),

    -- Seção 3: situação da obra. O esqueleto fixo garante as 6 categorias em toda
    -- UF, mesmo zeradas — a tabela do dashboard espera altura constante.
    skeleton_situacao(situacao, ordem) as (
        values
            ('CONCLUÍDO E ENTREGUE', 1),
            ('CONCLUÍDO', 2),
            ('EM ANDAMENTO', 3),
            ('PARALISADO', 4),
            ('NÃO INICIADO', 5),
            ('DESMOBILIZADO', 6)
    ),

    ufs as (select distinct uf, portaria, recorte_municipio from fichas),

    grade_situacao as (
        select u.uf, u.portaria, u.recorte_municipio, s.situacao, s.ordem
        from ufs u
        cross join skeleton_situacao s
    ),

    situacao_real as (
        select
            uf,
            portaria,
            recorte_municipio,
            situacao_empreendimento as situacao,
            count(apf) as qtd_empreendimentos,
            sum(quantidade_uh) as qtd_uhs
        from fichas
        group by uf, portaria, recorte_municipio, situacao_empreendimento
    ),

    dist_situacao as (
        select
            g.uf,
            g.portaria,
            g.recorte_municipio,
            g.situacao,
            g.ordem,
            coalesce(r.qtd_empreendimentos, 0) as qtd_empreendimentos,
            coalesce(r.qtd_uhs, 0) as qtd_uhs
        from grade_situacao g
        left join
            situacao_real r
            on g.uf = r.uf
            and g.portaria = r.portaria
            and g.recorte_municipio = r.recorte_municipio
            and g.situacao = r.situacao
    ),

    -- Seção 4: execução física por faixa percentual
    dist_execucao_fisica as (
        select
            uf,
            portaria,
            recorte_municipio,
            case
                when percentual_execucao_fisica <= 40
                then 'Menor que 40%'
                when percentual_execucao_fisica <= 55
                then 'De 41% a 55%'
                when percentual_execucao_fisica <= 75
                then 'De 56% a 75%'
                else 'Maior que 75%'
            end as faixa_execucao,
            count(apf) as qtd_empreendimentos,
            coalesce(sum(quantidade_uh), 0) as qtd_uhs
        from fichas
        group by
            uf,
            portaria,
            recorte_municipio,
            case
                when percentual_execucao_fisica <= 40
                then 'Menor que 40%'
                when percentual_execucao_fisica <= 55
                then 'De 41% a 55%'
                when percentual_execucao_fisica <= 75
                then 'De 56% a 75%'
                else 'Maior que 75%'
            end
    )

select
    h.uf,
    i.estado,
    h.portaria,
    h.recorte_municipio as municipio,
    'header' as secao,
    'resumo' as dimensao,
    0 as ordem,
    total_municipios as qtd_empreendimentos,
    total_uhs as qtd_uhs,
    total_valor_contratado as valor_1,
    total_valor_desembolsado as valor_2,
    total_valor_aporte_adicional as valor_3,
    valor_medio_por_uh as valor_4,
    total_empreendimentos as total_empreendimentos_uf
from header h
left join ibge_uf i on h.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    d.portaria,
    d.recorte_municipio as municipio,
    'metodo' as secao,
    metodo as dimensao,
    0 as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric as valor_1,
    null::numeric as valor_2,
    null::numeric as valor_3,
    null::numeric as valor_4,
    null::int as total_empreendimentos_uf
from dist_metodo d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    d.portaria,
    d.recorte_municipio as municipio,
    'portaria' as secao,
    portaria_selecao as dimensao,
    0 as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric,
    null::numeric,
    null::numeric,
    null::numeric,
    null::int
from dist_portaria d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    d.portaria,
    d.recorte_municipio as municipio,
    'demanda' as secao,
    demanda as dimensao,
    0 as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric,
    null::numeric,
    null::numeric,
    null::numeric,
    null::int
from dist_demanda d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    d.portaria,
    d.recorte_municipio as municipio,
    'situacao' as secao,
    situacao as dimensao,
    ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric,
    null::numeric,
    null::numeric,
    null::numeric,
    null::int
from dist_situacao d
left join ibge_uf i on d.uf = i.sigla

union all

select
    d.uf,
    i.estado,
    d.portaria,
    d.recorte_municipio as municipio,
    'execucao_fisica' as secao,
    faixa_execucao as dimensao,
    case
        faixa_execucao
        when 'Menor que 40%'
        then 1
        when 'De 41% a 55%'
        then 2
        when 'De 56% a 75%'
        then 3
        when 'Maior que 75%'
        then 4
    end as ordem,
    qtd_empreendimentos,
    qtd_uhs,
    null::numeric,
    null::numeric,
    null::numeric,
    null::numeric,
    null::int
from dist_execucao_fisica d
left join ibge_uf i on d.uf = i.sigla
