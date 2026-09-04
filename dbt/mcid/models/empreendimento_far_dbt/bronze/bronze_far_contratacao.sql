{{ config(materialized="table") }}

-- Bronze: Contratação FAR — cópia fiel de mcmv_staging.novo_mcmv_far_contratacao.
-- ATENÇÃO: fonte praticamente vazia (1 linha no snapshot atual). Materializada
-- para completude/linhagem; não consumida pela silver enquanto não popular.

with
    contratacao_raw as (
        select
            {{ normalize_apf('nu_apf') }} as apf,
            nullif(trim(no_identificacao_proposta), '') as id_proposta,
            nullif(trim(no_agente_financeiro), '') as agente_financeiro,
            coalesce(
                nullif(trim(no_nome_empreendimento), ''),
                nullif(trim(no_empreendimento_enquadramento), '')
            ) as empreendimento_nome,
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(no_uf), '') as uf,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,
            {{ parse_int('nu_qt_uh_empreendimento') }} as qt_uh,
            {{ parse_hist_numeric('vr_empreendimento_far') }} as valor_far,
            {{ parse_hist_numeric('vr_total_contrapartidas') }} as valor_contrapartidas,
            {{ parse_date_br('dt_protocolo') }} as dt_protocolo,
            {{ parse_date_br('dt_movimento') }} as dt_movimento,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_far_contratacao") }}
    )

select *
from contratacao_raw
