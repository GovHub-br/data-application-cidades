{{ config(materialized="table") }}

-- Gold: Panorama Estadual (Pró-Moradia) — grandes números por UF, sobre a ficha do contrato.
-- Grão: UF. Contratos cancelados/distratados entram na contagem total e na própria coluna,
-- mas NÃO nos valores e metas: somar o valor de contrato que deixou de existir inflaria a
-- carteira.

with
    ficha as (
        select * from {{ ref("ouro_pro_moradia_ficha_contrato") }}
    )

select
    uf,
    {{ regiao_da_uf("uf") }} as regiao,
    count(*) as qt_contratos,
    count(*) filter (where ic_contrato_vigente) as qt_contratos_vigentes,
    count(*) filter (where status_execucao_simplificado = 'Cancelado ou distratado') as qt_cancelados_distratados,
    count(*) filter (where status_execucao_simplificado = 'Concluído') as qt_concluidos,
    count(*) filter (where status_execucao_simplificado = 'Em Andamento') as qt_em_andamento,
    count(*) filter (where status_execucao_simplificado = 'Paralisado') as qt_paralisados,
    count(*) filter (where status_execucao_simplificado = 'Não Iniciado') as qt_nao_iniciados,
    count(*) filter (where status_execucao_simplificado = 'Sem Informação') as qt_sem_informacao,
    count(*) filter (where ic_contrato_vigente and tipo_intervencao = 'Urbanização') as qt_urbanizacao,
    count(*) filter (where ic_contrato_vigente and tipo_intervencao = 'Provisão habitacional') as qt_provisao_habitacional,
    count(distinct tomador_codigo) filter (where ic_contrato_vigente) as qt_tomadores,
    count(distinct cod_ibge) filter (where ic_contrato_vigente) as qt_municipios,
    coalesce(sum(quantidade_uh) filter (where ic_contrato_vigente), 0) as total_uh,
    coalesce(sum(populacao_beneficiada) filter (where ic_contrato_vigente), 0) as total_populacao_beneficiada,
    coalesce(sum(valor_contratado) filter (where ic_contrato_vigente), 0) as total_valor_contratado,
    coalesce(sum(valor_investimento) filter (where ic_contrato_vigente), 0) as total_valor_investimento,
    coalesce(sum(vr_desembolsado) filter (where ic_contrato_vigente), 0) as total_vr_desembolsado,
    case
        when sum(valor_contratado) filter (where ic_contrato_vigente) > 0
        then round(
            coalesce(sum(vr_desembolsado) filter (where ic_contrato_vigente), 0)
            / sum(valor_contratado) filter (where ic_contrato_vigente) * 100, 2
        )
    end as percentual_execucao_financeira
from ficha
where uf is not null
group by uf
