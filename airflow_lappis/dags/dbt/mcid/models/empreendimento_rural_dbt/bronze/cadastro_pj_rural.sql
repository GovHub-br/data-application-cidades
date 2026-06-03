{{ config(materialized="table") }}

-- Bronze: Cadastro PJ Rural
-- Fonte: novo_mcmv_rural_cad_pj_mensal
-- Saída: dados cadastrais limpos e tipados do empreendimento contratado

with
    cad_pj_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf_com_dv) as apf,
            nullif(trim(nu_contrato_empreend), '') as nu_contrato_empreend,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,
            nullif(trim(co_agente_finan), '') as agente_financeiro_codigo,
            
            -- Entidade Organizadora (EO)
            nullif(trim(no_nome_eo), '') as eo_nome,
            nullif(regexp_replace(trim(co_cnpj_eo), '[^0-9]', '', 'g'), '') as eo_cnpj,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(nu_ibge_empreend), '') as cod_ibge,

            -- Tipologia e Quantidades
            {{ parse_int('nu_modalidade') }} as modalidade_codigo,
            {{ parse_int('qt_uh_selecionadas') }} as qt_uh_selecionadas,
            {{ parse_int('qt_uh_contratadas') }} as qt_uh_contratadas,
            {{ parse_int('qt_uh_concluidas') }} as qt_uh_concluidas,
            
            -- Prazos
            {{ parse_int('pz_construcao') }} as prazo_construcao,

            -- Valores
            {{ parse_financial_value('vr_investimento_total') }} as vr_investimento_total,
            {{ parse_financial_value('vr_obra') }} as vr_obra,
            {{ parse_financial_value('vr_atec') }} as vr_atec,
            {{ parse_financial_value('vr_ts') }} as vr_trabalho_social,
            {{ parse_financial_value('vr_custo_indireto') }} as vr_custo_indireto,
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            {{ parse_financial_value('vr_aporte') }} as vr_aporte,
            {{ parse_financial_value('vr_suplementacao') }} as vr_suplementacao,

            -- Cisternas e Efluentes
            {{ parse_int('qt_cisterna') }} as qt_cisterna,
            {{ parse_financial_value('vr_cisterna') }} as vr_cisterna,
            {{ parse_int('qt_efluente') }} as qt_efluente,
            {{ parse_financial_value('vr_efluente') }} as vr_efluente,

            -- Execução Física
            {{ parse_numeric('pc_obra_realizada', 'numeric(6,2)') }} as percentual_obra_realizada,

            -- Situação da Obra
            {{ parse_int('co_situacao_obra') }} as co_situacao_obra,

            -- Datas-chave
            {{ target.schema }}.parse_date_br(dt_contratacao) as dt_contratacao,
            {{ target.schema }}.parse_date_br(dt_retomada) as dt_retomada,
            {{ target.schema }}.parse_date_br(dt_ult_liberacao) as dt_ultima_liberacao,
            {{ target.schema }}.parse_date_br(dt_conclusao) as dt_conclusao,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_rural_cad_pj_mensal") }}
    )

select *
from cad_pj_raw
