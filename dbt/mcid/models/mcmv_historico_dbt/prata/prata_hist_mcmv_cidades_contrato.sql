{{ config(materialized="table") }}

-- SILVER — contratos MCMV Cidades, união (D2 da change
-- frentes-restantes-mcmv-historico) das 2 bronzes complementares:
--
-- bronze_sftp_mcmv_cidades       (GEFUS, snapshot mensal, grão agregado por
--                                 ente público).
-- bronze_shpt_mcmv_cidades_emendas (SharePoint, consolidado transacional por
--                                 contrato).
--
-- As granularidades não são comparáveis — NÃO há tentativa de casar linha a
-- linha entre as duas fontes: cada uma contribui com sua própria fatia via
-- `union all`, identificada por `fonte_bronze`. Reconciliação fica como
-- follow-up (ver design.md, D2).
--
-- Dedup por (fonte_bronze, nu_contrato, dt_referencia), mesma disciplina de
-- prata_classe_media_/prata_hist_reforma_casa_brasil_contrato: a bronze
-- GEFUS tem reentregas do mesmo mês sob nomes diferentes (achado ao investigar
-- possível duplicação de contagens financeiras/UH — 192 linhas de 2026-02-06
-- duplicadas por `PMCMV_CIDADES_MCID_2026_02_06.parquet` +
-- `..._2026_02_06_0000.parquet`, dobrando o valor_contratado somado daquele
-- mês antes deste fix). O particionamento inclui `fonte_bronze` para nunca
-- colapsar linhas entre as 2 fontes (não há sobreposição real de
-- nu_contrato/numero_do_contrato entre elas, mas a chave deixa isso explícito
-- em vez de depender de uma coincidência observada).
{% set gefus = ref('bronze_sftp_mcmv_cidades') %}
{% set emendas = ref('bronze_shpt_mcmv_cidades_emendas') %}

with

    cidades_gefus as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'MCMV Cidades'::text as frente_mcmv,
            'pmcmv_cidades_gefus'::text as fonte_bronze,
            nullif(trim(nu_contrato), '')::text as nu_contrato,
            nullif(trim(no_ente_publico), '')::text as responsavel_nome,
            nullif(trim(no_municipio), '')::text as municipio,
            nullif(trim(nu_ibge), '')::text as codigo_ibge_municipio,
            nullif(trim(sg_uf), '')::text as uf,
            {{ parse_hist_numeric('valor') }} as valor_contratado,
            {{ parse_date_br('dt_contratacao') }} as dt_contratacao,
            dt_referencia,
            source_file,
            hash_linha,
            dt_ingest
        from {{ gefus }}
    ),

    cidades_emendas as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'MCMV Cidades'::text as frente_mcmv,
            'cidades_emendas_sharepoint'::text as fonte_bronze,
            nullif(trim(numero_do_contrato), '')::text as nu_contrato,
            nullif(trim(tomador_nome), '')::text as responsavel_nome,
            nullif(trim(municipio_nome), '')::text as municipio,
            nullif(trim(municipio_codigo), '')::text as codigo_ibge_municipio,
            nullif(trim(uf_sigla), '')::text as uf,
            {{ parse_hist_numeric('vlr_do_financiamento_bruto') }} as valor_contratado,
            {{ parse_date_br('data_da_contratacao') }} as dt_contratacao,
            dt_referencia,
            source_file,
            hash_linha,
            dt_ingest
        from {{ emendas }}
    ),

    unioned as (
        select *
        from cidades_gefus
        union all
        select *
        from cidades_emendas
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by fonte_bronze, nu_contrato, dt_referencia
                order by source_file desc
            ) as rn
        from unioned
    )

select
    programa,
    frente_mcmv,
    fonte_bronze,
    nu_contrato,
    responsavel_nome,
    municipio,
    codigo_ibge_municipio,
    uf,
    valor_contratado,
    dt_contratacao,
    dt_referencia,
    source_file,
    hash_linha,
    dt_ingest
from dedup
where rn = 1 and nu_contrato is not null
