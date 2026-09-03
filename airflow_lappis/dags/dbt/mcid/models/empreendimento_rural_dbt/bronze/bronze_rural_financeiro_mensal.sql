{{ config(materialized="table") }}

-- Bronze: Financeiro Mensal Rural — liberações financeiras por empreendimento
-- Fonte: mcmv_staging.novo_mcmv_rural_financeiro_mensal (staging/sharepoint)
-- Cópia fiel. APF fonte em 6 dígitos → normalize_apf (join na silver pela raiz de 6).

with
    financeiro_raw as (
        select
            {{ normalize_apf('nu_apf') }} as apf,
            nullif(trim(co_tipo_registro), '') as co_tipo_registro,
            nullif(trim(ic_credito), '') as ic_credito,

            {{ parse_hist_numeric('vr_movimento') }} as vr_movimento,
            {{ parse_hist_numeric('vr_desembolso_obra') }} as vr_desembolso_obra,
            {{ parse_hist_numeric('vr_desembolso_ts') }} as vr_desembolso_trabalho_social,
            {{ parse_hist_numeric('vr_desembolso_atec') }} as vr_desembolso_atec,
            {{ parse_hist_numeric('vr_desembolso_cisternas_efluentes') }} as vr_desembolso_cisternas_efluentes,
            {{ parse_hist_numeric('vr_desembolso_custos_indiretos') }} as vr_desembolso_custos_indiretos,

            {{ parse_numeric('pc_evolucao', 'numeric(6, 2)') }} as pct_evolucao,

            {{ parse_date_br('dt_movimento') }} as dt_movimento,
            {{ parse_date_br('dt_liberacao_recurso') }} as dt_liberacao,
            {{ parse_date_br('dt_remessa') }} as dt_remessa,
            nullif(trim(nu_identificador), '') as identificador,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_rural_financeiro_mensal") }}
    )

select *
from financeiro_raw
