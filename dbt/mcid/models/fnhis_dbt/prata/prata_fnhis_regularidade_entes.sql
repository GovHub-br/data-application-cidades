{{ config(materialized="table") }}

-- Prata: Regularidade dos entes no SNHIS
-- Fonte: bronze_shpt_snhis_regularidade_entes (extração semanal da CAIXA; a bronze guarda a
-- mais recente). Grão: um ente — município, estado ou DF —, identificado pelo IBGE de 7 dígitos
-- (os estados usam o código da UF seguido de zeros).
--
-- Estar regular no SNHIS é condição para receber recurso do FNHIS: é a checagem de "ente apto"
-- das Validações/Regras. São cinco exigências — lei do fundo local (FLHIS), lei do conselho
-- gestor, termo de adesão, plano habitacional e relatório de gestão —, e a situação do ente é a
-- síntese que a CAIXA calcula a partir delas.

with
    ente as (
        select
            nullif(trim(coibge::text), '') as cod_ibge,
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio::text)), '') as ente_nome,
            nullif(trim(uf::text), '') as uf,
            nullif(regexp_replace(cnpj::text, '[^0-9]', '', 'g'), '') as ente_cnpj,
            {{ parse_int("populacao::text") }} as populacao,
            upper(trim({{ target.schema }}.corrigir_mojibake(reg_metrop::text))) = 'SIM' as ic_regiao_metropolitana,
            nullif(trim(situacao_lei_flhis::text), '') as situacao_lei_fundo,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_lei_flhis_analise_cefus::text), '')) as dt_lei_fundo,
            nullif(trim(situacao_lei_cgflhis::text), '') as situacao_lei_conselho,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_lei_cgflhis_analise_cefus::text), '')) as dt_lei_conselho,
            nullif(trim(situacao_termo_adesao::text), '') as situacao_termo_adesao,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_termo_adesao_analise_cefus::text), '')) as dt_termo_adesao,
            nullif(trim(situacao_plano_habit::text), '') as situacao_plano_habitacional,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_plano_habit_analise_cefus::text), '')) as dt_plano_habitacional,
            {{ parse_int("ano_rel_gestao::text") }} as ano_relatorio_gestao,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_rel_gestao::text)), '') as situacao_relatorio_gestao,
            nullif(trim(situacao_municipio::text), '') as situacao_ente,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_referencia::text), '')) as dt_referencia,
            nullif(trim(_source_file::text), '') as arquivo_de_origem,
            nullif(trim(_ingested_at::text), '')::timestamp as criado_em
        from {{ ref("bronze_shpt_snhis_regularidade_entes") }}
    )

select
    cod_ibge,
    left(cod_ibge, 6) as cod_ibge_6,
    case
        when cod_ibge ~ '^\d{2}0{5}$' or ente_nome ~* '^GOVERNO DO' then 'Estado/DF'
        else 'Município'
    end as ente_esfera,
    ente_nome,
    uf,
    ente_cnpj,
    populacao,
    ic_regiao_metropolitana,
    situacao_lei_fundo,
    dt_lei_fundo,
    situacao_lei_conselho,
    dt_lei_conselho,
    situacao_termo_adesao,
    dt_termo_adesao,
    situacao_plano_habitacional,
    dt_plano_habitacional,
    ano_relatorio_gestao,
    situacao_relatorio_gestao,
    situacao_ente,
    situacao_ente = 'Regular' as ic_ente_regular,
    dt_referencia,
    arquivo_de_origem,
    criado_em
from ente
