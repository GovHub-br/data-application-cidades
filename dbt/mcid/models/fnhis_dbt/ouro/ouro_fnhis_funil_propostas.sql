{{ config(materialized="table") }}

-- Gold: Funil do FNHIS Sub-50 por UF — da proposta apresentada à obra entregue.
-- Grão: UF. Cada etapa conta a partir da sua própria fonte (propostas, termos do TransfereGov e
-- contratos da SNH), então as colunas não precisam fechar entre si: a diferença entre
-- selecionadas e termos ativos é o que não virou instrumento, e entre termos e contratos SNH é o
-- que a SNH ainda não acompanha.

with
    proposta as (
        select
            uf,
            count(*) as qt_propostas_apresentadas,
            coalesce(sum(quantidade_uh), 0) as uh_apresentadas,
            count(*) filter (where resultado_selecao = 'Não enquadrada') as qt_nao_enquadradas,
            count(*) filter (where resultado_selecao = 'Cota insuficiente da UF') as qt_cota_insuficiente,
            count(*) filter (where resultado_selecao = 'Município já contemplado') as qt_municipio_ja_contemplado,
            count(*) filter (where ic_selecionada) as qt_selecionadas,
            coalesce(sum(quantidade_uh) filter (where ic_selecionada), 0) as uh_selecionadas
        from {{ ref("prata_fnhis_propostas") }}
        where uf is not null
        group by uf
    ),

    termo as (
        select
            uf,
            count(*) as qt_termos,
            count(*) filter (where ic_instrumento_ativo) as qt_termos_ativos,
            count(*) filter (where situacao_instrumento in ('Convênio Anulado', 'Convênio Rescindido')) as qt_termos_anulados_rescindidos,
            coalesce(sum(valor_repasse) filter (where ic_instrumento_ativo), 0) as valor_repasse_termos_ativos,
            coalesce(sum(valor_empenhado_acumulado) filter (where ic_instrumento_ativo), 0) as valor_empenhado
        from {{ ref("prata_fnhis_termo_compromisso") }}
        where uf is not null
        group by uf
    ),

    contrato as (
        select
            uf,
            count(*) as qt_contratos_snh,
            coalesce(sum(uh_contratadas), 0) as uh_contratadas_snh,
            coalesce(sum(uh_entregues), 0) as uh_entregues_snh,
            count(*) filter (where percentual_execucao_fisica > 0) as qt_obras_iniciadas,
            coalesce(sum(valor_desembolsado), 0) as valor_desembolsado_snh
        from {{ ref("prata_fnhis_prioritarios_snh") }}
        where uf is not null
        group by uf
    ),

    ufs as (
        select uf from proposta
        union
        select uf from termo
        union
        select uf from contrato
    )

select
    u.uf,
    {{ regiao_da_uf("u.uf") }} as regiao,
    coalesce(p.qt_propostas_apresentadas, 0) as qt_propostas_apresentadas,
    coalesce(p.uh_apresentadas, 0) as uh_apresentadas,
    coalesce(p.qt_nao_enquadradas, 0) as qt_nao_enquadradas,
    coalesce(p.qt_cota_insuficiente, 0) as qt_cota_insuficiente,
    coalesce(p.qt_municipio_ja_contemplado, 0) as qt_municipio_ja_contemplado,
    coalesce(p.qt_selecionadas, 0) as qt_selecionadas,
    coalesce(p.uh_selecionadas, 0) as uh_selecionadas,
    case
        when p.qt_propostas_apresentadas > 0
        then round(p.qt_selecionadas::numeric / p.qt_propostas_apresentadas * 100, 2)
    end as percentual_selecao,
    coalesce(t.qt_termos, 0) as qt_termos,
    coalesce(t.qt_termos_ativos, 0) as qt_termos_ativos,
    coalesce(t.qt_termos_anulados_rescindidos, 0) as qt_termos_anulados_rescindidos,
    coalesce(t.valor_repasse_termos_ativos, 0) as valor_repasse_termos_ativos,
    coalesce(t.valor_empenhado, 0) as valor_empenhado,
    coalesce(c.qt_contratos_snh, 0) as qt_contratos_snh,
    coalesce(c.uh_contratadas_snh, 0) as uh_contratadas_snh,
    coalesce(c.qt_obras_iniciadas, 0) as qt_obras_iniciadas,
    coalesce(c.uh_entregues_snh, 0) as uh_entregues_snh,
    coalesce(c.valor_desembolsado_snh, 0) as valor_desembolsado_snh
from ufs u
left join proposta p on p.uf = u.uf
left join termo t on t.uf = u.uf
left join contrato c on c.uf = u.uf
