{{ config(materialized="table") }}

-- SILVER — propostas FNHIS/SUB50, união das 2 bronzes (apresentadas /
-- selecionadas), com `status_proposta` distinguindo a origem. Contrato de
-- colunas segue docs/entregas/issue-119-ajuste-frentes-faltantes.md (mesmo
-- mapeamento do modelo de referência desativado
-- mcmv_silver_dbt/silver/sub50/silver_mcmv_sub50_base.sql, portado para
-- DuckDB puro — parse_hist_numeric/parse_hist_bigint/parse_date_br no lugar
-- das UDFs Postgres `parse_financial_value`/`parse_int`/`parse_date_br`).
{% set apresentadas = ref('bronze_shpt_sub50_propostas_apresentadas') %}
{% set selecionadas = ref('bronze_shpt_sub50_propostas_selecionadas') %}

with

    sub50_apresentadas as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'FNHIS/SUB50'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'FNHIS SUB50'::text as linha_mcmv,
            'apresentada'::text as status_proposta,
            nullif(trim(numero_da_proposta), '')::text as contrato,
            nullif(trim(numero_da_proposta), '')::text as codigo_empreendimento,
            nullif(trim(cod_ibge_munic_beneficiado), '')::text as codigo_ibge_municipio,
            nullif(trim(municipio), '')::text as municipio,
            null::text as uf,
            'Proponente'::text as responsavel_tipo,
            null::text as responsavel_id,
            nullif(trim(proponente), '')::text as responsavel_nome,
            {{ parse_hist_bigint('total_de_uh') }} as quantidade_uh,
            null::numeric(15, 2) as valor_contratado,
            null::numeric(15, 2) as valor_desembolsado,
            coalesce(
                nullif(trim(situacao_da_proposta), ''),
                nullif(trim(justificativa_nao_enquadramento), '')
            )::text as status_operacional,
            dt_referencia,
            null::date as dt_contratacao,
            dt_referencia as dt_ultima_atualizacao,
            current_timestamp as dt_silver,
            source_file,
            hash_linha,
            dt_ingest
        from {{ apresentadas }}
        where nullif(trim(numero_da_proposta), '') is not null
    ),

    sub50_selecionadas as (
        select
            'Minha Casa Minha Vida'::text as programa,
            'FNHIS/SUB50'::text as frente_mcmv,
            'Subsidiada'::text as grupo_linha,
            'FNHIS SUB50'::text as linha_mcmv,
            'selecionada'::text as status_proposta,
            nullif(trim(num_proposta), '')::text as contrato,
            nullif(trim(num_proposta), '')::text as codigo_empreendimento,
            null::text as codigo_ibge_municipio,
            nullif(trim(municipio), '')::text as municipio,
            nullif(trim(uf), '')::text as uf,
            'Proponente'::text as responsavel_tipo,
            nullif(trim(cnpj), '')::text as responsavel_id,
            nullif(trim(nome_proponente), '')::text as responsavel_nome,
            null::bigint as quantidade_uh,
            {{ parse_hist_numeric(
                "coalesce(valor_de_repasse, vl_repasse_proposta, valor_empenhado_acumulado)"
            ) }} as valor_contratado,
            {{ parse_hist_numeric(
                "coalesce(valor_empenhado_acumulado, vl_empenhado_pre_convenio)"
            ) }} as valor_desembolsado,
            coalesce(
                nullif(trim(sit_contratacao), ''),
                nullif(trim(situacao_proposta), ''),
                nullif(trim(situacao_instrumento), ''),
                nullif(trim(modalidade), '')
            )::text as status_operacional,
            dt_referencia,
            {{ parse_date_br('data_assinatura') }} as dt_contratacao,
            coalesce({{ parse_date_br('data_assinatura') }}, dt_referencia)
            as dt_ultima_atualizacao,
            current_timestamp as dt_silver,
            source_file,
            hash_linha,
            dt_ingest
        from {{ selecionadas }}
        where nullif(trim(num_proposta), '') is not null
    )

select *
from sub50_apresentadas
union all
select *
from sub50_selecionadas
