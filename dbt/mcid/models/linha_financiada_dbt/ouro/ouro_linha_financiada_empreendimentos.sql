{{ config(materialized='table') }}

with
    contratos as (
        select
            codigo_empreendimento,
            count(*) as quantidade_contratos,
            sum(coalesce(valor_financiamento, 0)) as valor_financiamento,
            sum(
                coalesce(valor_desconto_fgts, 0) + coalesce(valor_desconto_ogu, 0)
            ) as valor_descontos,
            max(municipio) filter (where municipio is not null) as municipio_contrato,
            max(uf) filter (where uf is not null) as uf_contrato,
            bool_or(ic_pro_moradia) as ic_pro_moradia,
            bool_or(ic_mcmv_cidades) as ic_mcmv_cidades,
            bool_or(ic_classe_media) as ic_classe_media
        from {{ ref('ouro_linha_financiada_base_agregada') }}
        where codigo_empreendimento is not null
        group by 1
    ),

    empreendimento_unico as (
        -- Para a ficha da linha financiada, o código AO1 é a referência do
        -- empreendimento vinculável à operação CCA/PF. O arquivo PJ permanece
        -- apoio à produção e não pode substituir essa referência de contratação.
        select distinct on (codigo_empreendimento) *
        from {{ ref('prata_linha_financiada_empreendimento') }}
        order by
            codigo_empreendimento,
            case when fonte = 'FGTS_AO1' then 0 else 1 end,
            competencia_posicao desc nulls last
    )
-- Colunas da prata listadas uma a uma, sem `e.*`: o CNPJ da entidade PJ (`cnpj_entidade_pj`)
-- fica só na prata. Ele chegava aqui pelo `*` sem ninguém escolher, e a ouro não publica
-- CNPJ (tests/linha_financiada/linha_financiada_ouro_sem_identificador_pessoal.sql).
select
    e.id_empreendimento_linha_financiada,
    e.codigo_empreendimento,
    e.nome_empreendimento,
    e.codigo_municipio,
    e.municipio,
    e.uf,
    e.localidade,
    e.objeto,
    e.quantidade_unidades,
    e.quantidade_unidades_concluidas,
    e.quantidade_unidades_entregues,
    e.percentual_obra,
    e.competencia_posicao,
    e.data_inicio_raw,
    e.data_termino_raw,
    e.data_inauguracao_raw,
    e.latitude,
    e.longitude,
    e.valor_investimento_pj,
    e.valor_contratado_pj,
    e.situacao_contrato_pj,
    e.entidade_pj,
    e.fonte,
    e.dt_silver,
    coalesce(e.municipio, c.municipio_contrato) as municipio_painel,
    coalesce(e.uf, c.uf_contrato) as uf_painel,
    coalesce(c.quantidade_contratos, 0) as quantidade_contratos,
    coalesce(c.valor_financiamento, 0) as valor_financiamento,
    coalesce(c.valor_descontos, 0) as valor_descontos,
    case
        when coalesce(e.quantidade_unidades, 0) > 0
        then round(coalesce(c.quantidade_contratos, 0)::numeric / e.quantidade_unidades * 100, 2)
    end as percentual_unidades_com_pf,
    coalesce(c.ic_pro_moradia, false) as ic_pro_moradia,
    coalesce(c.ic_mcmv_cidades, false) as ic_mcmv_cidades,
    coalesce(c.ic_classe_media, false) as ic_classe_media,
    case
        when e.percentual_obra >= 100
        then 'Concluído'
        when e.percentual_obra > 0
        then 'Em execução'
        when e.percentual_obra = 0
        then 'Não iniciado'
        else 'Sem posição de obra'
    end as situacao_execucao,
    current_timestamp as dt_gold
from empreendimento_unico e
left join contratos c using (codigo_empreendimento)
