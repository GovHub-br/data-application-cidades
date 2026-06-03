{{ config(materialized="table") }}

-- Bronze: Integração BB PJ (PNHR)
-- Fonte: int_empreendimentos_int_057_pnhr_bb_pj
-- Saída: dados cadastrais e financeiros históricos do BB PJ limpos e tipados

with
    int_bb_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(nu_contrato_empreendimento) as apf,
            nullif(trim(nu_contrato_empreendimento), '') as nu_contrato_empreendimento,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,
            nullif(trim(co_agente_financeiro), '') as agente_financeiro_codigo,

            -- Entidade Organizadora (EO)
            nullif(trim(no_entidade_organizadora), '') as eo_nome,
            nullif(regexp_replace(trim(nu_cnpj_entidade), '[^0-9]', '', 'g'), '') as eo_cnpj,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,

            -- Quantidades
            {{ parse_int('qt_unidades') }} as qt_unidades,
            {{ parse_int('qt_unidades_concluidas') }} as qt_unidades_concluidas,
            {{ parse_int('qt_unidades_entregues') }} as qt_unidades_entregues,

            -- Valores
            {{ parse_financial_value('vr_investimento') }} as vr_investimento,
            {{ parse_financial_value('vr_operacao') }} as vr_operacao,
            {{ parse_financial_value('vr_edificacao') }} as vr_edificacao,
            {{ parse_financial_value('vr_atec') }} as vr_atec,
            {{ parse_financial_value('vr_ts') }} as vr_trabalho_social,
            {{ parse_financial_value('vr_custo_originacao') }} as vr_custo_originacao,
            {{ parse_financial_value('vr_taxa_administracao') }} as vr_taxa_administracao,
            {{ parse_financial_value('vr_diferencial_juros') }} as vr_diferencial_juros,
            {{ parse_financial_value('vr_taxa_risco_credito') }} as vr_taxa_risco_credito,
            {{ parse_financial_value('vr_cisterna') }} as vr_cisterna,
            {{ parse_financial_value('vr_efluentes') }} as vr_efluentes,
            {{ parse_financial_value('vr_contrapartida') }} as vr_contrapartida,
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,

            -- Prazos e Execução
            {{ parse_int('pz_obra') }} as prazo_obra,
            {{ parse_numeric('pc_execucao_fisica_obra', 'numeric(6, 2)') }} as percentual_execucao_fisica,
            {{ parse_numeric('pc_execucao_financeira_obra', 'numeric(6, 2)') }} as percentual_execucao_financeira,
            nullif(trim(no_situacao_obra), '') as situacao_obra,

            -- Datas
            case
                when dt_contrato is null or trim(dt_contrato) = '' then null
                when dt_contrato ~ '^\d{4}-\d{2}-\d{2}' then dt_contrato::date
                else {{ target.schema }}.parse_date_br(dt_contrato)
            end as dt_contrato,
            case
                when dt_ultima_liberacao is null or trim(dt_ultima_liberacao) = '' then null
                when dt_ultima_liberacao ~ '^\d{4}-\d{2}-\d{2}' then dt_ultima_liberacao::date
                else {{ target.schema }}.parse_date_br(dt_ultima_liberacao)
            end as dt_ultima_liberacao,
            case
                when dt_efetiva_conclusao is null or trim(dt_efetiva_conclusao) = '' then null
                when dt_efetiva_conclusao ~ '^\d{4}-\d{2}-\d{2}' then dt_efetiva_conclusao::date
                else {{ target.schema }}.parse_date_br(dt_efetiva_conclusao)
            end as dt_conclusao_obra,
            case
                when dt_movimento is null or trim(dt_movimento) = '' then null
                when dt_movimento ~ '^\d{4}-\d{2}-\d{2}' then dt_movimento::date
                else {{ target.schema }}.parse_date_br(dt_movimento)
            end as dt_movimento,

            -- Metadados
            nullif(trim(origem), '') as origem,
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "int_empreendimentos_int_057_pnhr_bb_pj") }}
    )

select *
from int_bb_raw
