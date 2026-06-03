{{ config(materialized="table") }}

-- Bronze: Cadastro PF Rural (Beneficiários)
-- Fonte: novo_mcmv_rural_cadastro_pf_mensal
-- Saída: dados socioeconômicos dos beneficiários do programa Rural limpos e tipados

with
    cad_pf_raw as (
        select
            -- Identificadores
            nullif(trim(nu_registro), '') as nu_registro,
            {{ target.schema }}.normalize_apf(nu_apf_com_dv) as apf,
            nullif(trim(nu_contrato_empreendimento), '') as nu_contrato_empreendimento,
            nullif(trim(nu_contrato_nidividual), '') as nu_contrato_individual,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,
            
            -- Entidade Organizadora (EO)
            nullif(trim(no_eo_empreendimento), '') as eo_nome,
            nullif(regexp_replace(trim(co_cnpj_eo), '[^0-9]', '', 'g'), '') as eo_cnpj,

            -- Localização
            nullif(trim(no_end_beneficiario), '') as endereco_beneficiario,
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(nu_municipio_ibge), '') as cod_ibge,

            -- Dados do Beneficiário
            nullif(trim(no_beneficiario), '') as beneficiario_nome,
            nullif(regexp_replace(trim(nu_cpf_beneficiario), '[^0-9]', '', 'g'), '') as beneficiario_cpf,
            nullif(trim(co_sexo_benef), '') as beneficiario_sexo,
            {{ parse_int('nu_estado_civil') }} as estado_civil_codigo,
            nullif(trim(no_tipo_beneficiario), '') as tipo_beneficiario,
            {{ parse_int('co_sit_funcidaria') }} as situacao_funcionaria_codigo,

            -- Demografia e Indicadores Sociais
            {{ parse_int('qt_pessoas_familia') }} as qt_pessoas_familia,
            case when trim(ic_benef_bpc) = 'S' then true else false end as ic_benef_bpc,
            case when trim(ic_benef_bf) = 'S' then true else false end as ic_benef_bolsa_familia,

            -- Valores
            {{ parse_financial_value('vr_renda_familiar') }} as vr_renda_familiar,
            {{ parse_financial_value('vr_imovel') }} as vr_imovel,
            {{ parse_financial_value('vr_subsidio_uh') }} as vr_subsidio_uh,
            {{ parse_financial_value('vr_contrapartida_uh') }} as vr_contrapartida_uh,
            {{ parse_financial_value('vr_caucao') }} as vr_caucao,
            {{ parse_financial_value('vr_distrato') }} as vr_distrato,

            -- Saneamento Rural
            {{ parse_int('qt_cisterna') }} as qt_cisterna,
            {{ parse_int('qt_efluentes') }} as qt_efluente,

            -- Distrato
            case when trim(ic_distrato) = 'S' then true else false end as ic_distrato,
            co_motivo_distrato,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_contratacao) as dt_contratacao,
            {{ target.schema }}.parse_date_br(dt_nascimento) as dt_nascimento,
            {{ target.schema }}.parse_date_br(dt_recolhimento_caucao) as dt_recolhimento_caucao,
            {{ target.schema }}.parse_date_br(dt_distrato) as dt_distrato,
            {{ target.schema }}.parse_date_br(dt_efetiva_conclusao) as dt_efetiva_conclusao,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_rural_cadastro_pf_mensal") }}
    )

select *
from cad_pf_raw
