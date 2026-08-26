{{ config(materialized="table") }}

with
    periodos as (
        select
            ano,
            trimestre,
            cbic_vendas_regiao_norte as total_norte,
            cbic_vendas_mcmv_regiao_norte as mcmv_norte,
            cbic_vendas_mcmv_perc_regiao_norte as perc_norte,
            cbic_vendas_regiao_nordeste as total_nordeste,
            cbic_vendas_mcmv_regiao_nordeste as mcmv_nordeste,
            cbic_vendas_mcmv_perc_regiao_nordeste as perc_nordeste,
            cbic_vendas_regiao_centro_oeste as total_centro_oeste,
            cbic_vendas_mcmv_regiao_centro_oeste as mcmv_centro_oeste,
            cbic_vendas_mcmv_perc_regiao_centro_oeste as perc_centro_oeste,
            cbic_vendas_regiao_sudeste as total_sudeste,
            cbic_vendas_mcmv_regiao_sudeste as mcmv_sudeste,
            cbic_vendas_mcmv_perc_regiao_sudeste as perc_sudeste,
            cbic_vendas_regiao_sul as total_sul,
            cbic_vendas_mcmv_regiao_sul as mcmv_sul,
            cbic_vendas_mcmv_perc_regiao_sul as perc_sul,
            dt_ingest,
            dt_silver
        from {{ ref("silver_cbic_lancamentos_vendas") }}
    ),

    regioes as (
        select
            'NORTE' as regiao,
            total_norte as total,
            mcmv_norte as mcmv,
            perc_norte as perc_mcmv,
            ano,
            trimestre,
            dt_ingest,
            dt_silver
        from periodos
        union all
        select
            'NORDESTE',
            total_nordeste,
            mcmv_nordeste,
            perc_nordeste,
            ano,
            trimestre,
            dt_ingest,
            dt_silver
        from periodos
        union all
        select
            'CENTRO-OESTE',
            total_centro_oeste,
            mcmv_centro_oeste,
            perc_centro_oeste,
            ano,
            trimestre,
            dt_ingest,
            dt_silver
        from periodos
        union all
        select
            'SUDESTE',
            total_sudeste,
            mcmv_sudeste,
            perc_sudeste,
            ano,
            trimestre,
            dt_ingest,
            dt_silver
        from periodos
        union all
        select 'SUL', total_sul, mcmv_sul, perc_sul, ano, trimestre, dt_ingest, dt_silver
        from periodos
    ),

    periodo_atual as (
        select regiao, total, mcmv, perc_mcmv, ano, trimestre, dt_ingest, dt_silver
        from regioes
        where ano = 2025 and trimestre = 4
    ),

    periodo_anterior as (
        select regiao, perc_mcmv from regioes where ano = 2025 and trimestre = 3
    ),

    periodo_ano_anterior as (
        select regiao, perc_mcmv from regioes where ano = 2024 and trimestre = 4
    )

select
    atual.regiao,
    atual.total,
    atual.mcmv,
    round(atual.perc_mcmv::numeric, 1) as perc_mcmv_4t25,
    round(ant.perc_mcmv::numeric, 1) as perc_mcmv_3t25,
    round(aa.perc_mcmv::numeric, 1) as perc_mcmv_4t24,
    {{ add_metadata_timestamps("gold") }}
from periodo_atual atual
left join periodo_anterior ant on atual.regiao = ant.regiao
left join periodo_ano_anterior aa on atual.regiao = aa.regiao
order by
    case
        atual.regiao
        when 'NORTE'
        then 1
        when 'NORDESTE'
        then 2
        when 'CENTRO-OESTE'
        then 3
        when 'SUDESTE'
        then 4
        when 'SUL'
        then 5
    end
