{{ config(materialized="table") }}

with
    pivotado as (
        select
            data_referencia,
            max(case when tipo = 'pf_concessoes_rs_mi' then valor end) as pf_concessoes,
            max(case when tipo = 'pf_taxa_juros_aa' then valor end) as pf_taxa_juros,
            max(
                case when tipo = 'pf_inadimplencia_pct' then valor end
            ) as pf_inadimplencia,
            max(case when tipo = 'pj_concessoes_rs_mi' then valor end) as pj_concessoes,
            max(case when tipo = 'pj_taxa_juros_aa' then valor end) as pj_taxa_juros,
            max(
                case when tipo = 'pj_inadimplencia_pct' then valor end
            ) as pj_inadimplencia,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_bacen_financiamentos_imobiliarios") }}
        group by data_referencia
    ),

    dez25 as (
        select
            pf_concessoes,
            pf_taxa_juros,
            pf_inadimplencia,
            pj_concessoes,
            pj_taxa_juros,
            pj_inadimplencia,
            dt_ingest,
            dt_silver
        from pivotado
        where data_referencia = '202501-12-01'
    ),

    nov25 as (
        select
            pf_concessoes,
            pf_taxa_juros,
            pf_inadimplencia,
            pj_concessoes,
            pj_taxa_juros,
            pj_inadimplencia,
            dt_ingest,
            dt_silver
        from pivotado
        where data_referencia = '202501-11-01'
    ),

    dez24 as (
        select
            pf_concessoes,
            pf_taxa_juros,
            pf_inadimplencia,
            pj_concessoes,
            pj_taxa_juros,
            pj_inadimplencia,
            dt_ingest,
            dt_silver
        from pivotado
        where data_referencia = '202401-12-01'
    ),

    acum_dez25 as (
        select
            sum(pf_concessoes) as pf_concessoes,
            sum(pj_concessoes) as pj_concessoes,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from pivotado
        where data_referencia between '202501-01-01' and '202501-12-01'
    ),

    acum_dez24 as (
        select
            sum(pf_concessoes) as pf_concessoes,
            sum(pj_concessoes) as pj_concessoes,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from pivotado
        where data_referencia between '202401-01-01' and '202401-12-01'
    ),

    resultado as (
        select
            'dez/25' as periodo,
            d25.pf_concessoes,
            d25.pf_taxa_juros,
            d25.pf_inadimplencia,
            d25.pj_concessoes,
            d25.pj_taxa_juros,
            d25.pj_inadimplencia,
            d25.dt_ingest,
            d25.dt_silver
        from dez25 d25
        union all
        select
            'nov/25',
            n25.pf_concessoes,
            n25.pf_taxa_juros,
            n25.pf_inadimplencia,
            n25.pj_concessoes,
            n25.pj_taxa_juros,
            n25.pj_inadimplencia,
            n25.dt_ingest,
            n25.dt_silver
        from nov25 n25
        union all
        select
            'dez/24',
            d24.pf_concessoes,
            d24.pf_taxa_juros,
            d24.pf_inadimplencia,
            d24.pj_concessoes,
            d24.pj_taxa_juros,
            d24.pj_inadimplencia,
            d24.dt_ingest,
            d24.dt_silver
        from dez24 d24
        union all
        select
            '12 meses - dez/25',
            a25.pf_concessoes,
            null,
            null,
            a25.pj_concessoes,
            null,
            null,
            a25.dt_ingest,
            a25.dt_silver
        from acum_dez25 a25
        union all
        select
            '12 meses - dez/24',
            a24.pf_concessoes,
            null,
            null,
            a24.pj_concessoes,
            null,
            null,
            a24.dt_ingest,
            a24.dt_silver
        from acum_dez24 a24
    )

select
    periodo,
    pf_concessoes,
    pf_taxa_juros,
    pf_inadimplencia,
    pj_concessoes,
    pj_taxa_juros,
    pj_inadimplencia,
    {{ add_metadata_timestamps("gold") }}
from resultado
