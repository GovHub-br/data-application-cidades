{{ config(materialized="table") }}

-- BRONZE — serie executiva historica do MCMV (pre-2024), copia fiel.
--
-- Empilha, SEM regra de negocio, quatro familias de relatorios do dump
-- `staging/dados_historicos/` que trazem UH contratadas/entregues, valores e o
-- split OGU/FGTS por UF/municipio/faixa, cobrindo ~2010-2018 — o buraco que a
-- serie SNH (2024-06+) nao cobre. Base da analise preditiva (tendencia,
-- sazonalidade, drift, backtest do relogio) e candidata a substituir o seed do
-- piloto #118.
--
-- Familias (glob em staging/dados_historicos/):
-- bases_relatorio_executivo  *bases_relat*rio_executivo*   grao empreendimento
-- min_cidades                *min_cidades*                 grao empreendimento/contrato
-- (BB)
-- entrada_bb                 *entrada_bb*                   grao empreendimento (BB)
-- bext                       *bext*                         grao contrato PF (CAIXA)
--
-- Cada familia tem 2-3 geracoes de schema; a harmonizacao (mapa de colunas) fica
-- na silver silver_mcmv_historico_serie_executiva. Aqui: union_by_name + colunas
-- de auditoria. `dt_referencia` = mes-snapshot: report_date normalizado, com
-- fallback pelo nome do arquivo.
--
-- Target obrigatorio: staging_duckdb (gating em dbt_project.yml).
with

    bases_rel as (
        select *, 'bases_relatorio_executivo' as fonte_familia, filename as source_file
        from
            {{ read_minio_staging_parquet_series('dados_historicos/*bases_relat*rio_executivo*.parquet') }}
    ),

    min_cidades as (
        select *, 'min_cidades' as fonte_familia, filename as source_file
        from
            {{ read_minio_staging_parquet_series('dados_historicos/*min_cidades*.parquet') }}
    ),

    entrada_bb as (
        select *, 'entrada_bb' as fonte_familia, filename as source_file
        from
            {{ read_minio_staging_parquet_series('dados_historicos/*entrada_bb*.parquet') }}
    ),

    bext as (
        select *, 'bext' as fonte_familia, filename as source_file
        from {{ read_minio_staging_parquet_series('dados_historicos/*bext*.parquet') }}
    ),

    unido as (
        select *
        from bases_rel
        union all by name
        select *
        from min_cidades
        union all by name
        select *
        from entrada_bb
        union all by name
        select *
        from bext
    )

select
    *,
    {{ parse_hist_date('report_date') }} as report_date_parsed,
    {{ hist_dt_referencia('report_date', 'source_file') }} as dt_referencia,
    current_timestamp as dt_ingest,
    md5(
        concat_ws(
            '|',
            source_file,
            coalesce(cast(content_hash as varchar), ''),
            cast(row_number() over (partition by source_file) as varchar)
        )
    ) as hash_linha
from unido
