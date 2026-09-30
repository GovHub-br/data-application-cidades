{{ config(materialized='table') }}

{# PostgreSQL não implementa UNION ALL BY NAME. A seleção explícita preserva
   a união por nome dos quatro layouts e permite a carga direta do MinIO. #}
{% if target.type == 'postgres' %}
    select
        r['ano_dotacao']::text as ano_dotacao, r['programa']::text as programa,
        r['uf_codigo']::text as uf_codigo, r['orcamento_original']::text as orcamento_original,
        r['orcamento_final']::text as orcamento_final, r['orcamento_alocado']::text as orcamento_alocado,
        r['arquivo_de_origem']::text as arquivo_de_origem, r['criado_em']::text as criado_em,
        r['_source_file']::text as _source_file, r['_ingested_at']::text as _ingested_at,
        r['_source_hash']::text as _source_hash, null::text as agrupamento
    from {{ fonte_lake('orcamento_fgts_2021', 'linha_financiada_lake') }} r
    union all
    select
        r['ano_dotacao']::text as ano_dotacao, r['programa']::text as programa,
        null::text as uf_codigo, r['orcamento_original']::text as orcamento_original,
        r['orcamento_final']::text as orcamento_final, r['orcamento_alocado']::text as orcamento_alocado,
        r['arquivo_de_origem']::text as arquivo_de_origem, r['criado_em']::text as criado_em,
        r['_source_file']::text as _source_file, r['_ingested_at']::text as _ingested_at,
        r['_source_hash']::text as _source_hash, r['agrupamento']::text as agrupamento
    from {{ fonte_lake('orcamento_fgts_2022', 'linha_financiada_lake') }} r
    union all
    select
        r['ano_dotacao']::text as ano_dotacao, r['programa']::text as programa,
        null::text as uf_codigo, r['orcamento_original']::text as orcamento_original,
        r['orcamento_final']::text as orcamento_final, r['orcamento_alocado']::text as orcamento_alocado,
        r['arquivo_de_origem']::text as arquivo_de_origem, r['criado_em']::text as criado_em,
        r['_source_file']::text as _source_file, r['_ingested_at']::text as _ingested_at,
        r['_source_hash']::text as _source_hash, r['agrupamento']::text as agrupamento
    from {{ fonte_lake('orcamento_fgts_2023', 'linha_financiada_lake') }} r
    union all
    select
        r['ano_dotacao']::text as ano_dotacao, r['programa']::text as programa,
        null::text as uf_codigo, r['orcamento_original']::text as orcamento_original,
        r['orcamento_final']::text as orcamento_final, r['orcamento_alocado']::text as orcamento_alocado,
        r['arquivo_de_origem']::text as arquivo_de_origem, r['criado_em']::text as criado_em,
        r['_source_file']::text as _source_file, r['_ingested_at']::text as _ingested_at,
        r['_source_hash']::text as _source_hash, r['agrupamento']::text as agrupamento
    from {{ fonte_lake('orcamento_fgts_2024', 'linha_financiada_lake') }} r
{% else %}
    select * from {{ fonte_lake('orcamento_fgts_2021', 'linha_financiada_lake') }}
    union all by name
    select * from {{ fonte_lake('orcamento_fgts_2022', 'linha_financiada_lake') }}
    union all by name
    select * from {{ fonte_lake('orcamento_fgts_2023', 'linha_financiada_lake') }}
    union all by name
    select * from {{ fonte_lake('orcamento_fgts_2024', 'linha_financiada_lake') }}
{% endif %}
