{{ config(materialized="table") }}

-- Bronze: Cadastro PF Rural — beneficiários (grão: 1 linha por contrato individual)
-- Fonte: mcmv_staging.novo_mcmv_rural_cadastro_pf_mensal (staging/sharepoint)
-- Cópia fiel. Chave de empreendimento: apf (de nu_apf_com_dv) + nu_contrato_empreendimento.

with
    cad_pf_raw as (
        select
            {{ normalize_apf('nu_apf_com_dv') }} as apf,
            nullif(trim(nu_contrato_empreendimento), '') as nu_contrato_empreendimento,
            nullif(trim(nu_contrato_nidividual), '') as nu_contrato_individual,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,
            nullif(trim(no_eo_empreendimento), '') as eo_nome,
            nullif(regexp_replace(trim(co_cnpj_eo), '[^0-9]', '', 'g'), '') as eo_cnpj,

            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(nu_municipio_ibge), '') as cod_ibge,

            nullif(regexp_replace(trim(nu_cpf_beneficiario), '[^0-9]', '', 'g'), '') as cpf_beneficiario,
            nullif(trim(no_beneficiario), '') as beneficiario_nome,
            nullif(trim(co_sexo_benef), '') as sexo_beneficiario,
            {{ parse_int('qt_pessoas_familia') }} as qt_pessoas_familia,
            nullif(trim(no_tipo_beneficiario), '') as tipo_beneficiario,
            nullif(trim(ic_benef_bpc), '') as ic_benef_bpc,
            nullif(trim(ic_benef_bf), '') as ic_benef_bolsa_familia,

            {{ parse_hist_numeric('vr_renda_familiar') }} as vr_renda_familiar,
            {{ parse_hist_numeric('vr_imovel') }} as vr_imovel,
            {{ parse_hist_numeric('vr_subsidio_uh') }} as vr_subsidio_uh,
            {{ parse_hist_numeric('vr_contrapartida_uh') }} as vr_contrapartida_uh,
            {{ parse_hist_numeric('vr_caucao') }} as vr_caucao,

            nullif(trim(ic_distrato), '') as ic_distrato,
            {{ parse_date_br('dt_distrato') }} as dt_distrato,
            {{ parse_hist_numeric('vr_distrato') }} as vr_distrato,

            {{ parse_date_br('dt_contratacao') }} as dt_contratacao,
            {{ parse_date_br('dt_nascimento') }} as dt_nascimento,
            {{ parse_date_br('dt_efetiva_conclusao') }} as dt_conclusao,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_rural_cadastro_pf_mensal") }}
    )

select *
from cad_pf_raw
