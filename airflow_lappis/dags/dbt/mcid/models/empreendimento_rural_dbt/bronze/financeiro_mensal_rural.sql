{{ config(materialized="table") }}

-- Bronze: Série histórica de desembolsos financeiros (Rural)
-- Fonte: novo_mcmv_rural_financeiro_mensal
-- Saída: liberações financeiras limpas e tipadas por APF

with
    financeiro_mensal_raw as (
        select
            -- Identificadores
            nullif(trim(id), '') as id,
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            {{ parse_int('co_tipo_registro') }} as co_tipo_registro,
            nullif(trim(nu_identificador), '') as nu_identificador,

            -- Execução e Crédito
            {{ parse_numeric('pc_evolucao', 'numeric(6,2)') }} as percentual_evolucao,
            nullif(trim(ic_credito), '') as ic_credito,

            -- Valores
            {{ parse_financial_value('vr_movimento') }} as vr_movimento,
            {{ parse_financial_value('vr_desembolso_obra') }} as vr_desembolso_obra,
            {{ parse_financial_value('vr_desembolso_ts') }} as vr_desembolso_trabalho_social,
            {{ parse_financial_value('vr_desembolso_atec') }} as vr_desembolso_atec,
            {{ parse_financial_value('vr_desembolso_cisternas_efluentes') }} as vr_desembolso_cisternas_efluentes,
            {{ parse_financial_value('vr_desembolso_custos_indiretos') }} as vr_desembolso_custos_indiretos,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_remessa) as dt_remessa,
            {{ target.schema }}.parse_date_br(dt_liberacao_recurso) as dt_liberacao_recurso,
            {{ target.schema }}.parse_date_br(dh_gravacao) as dh_gravacao,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_rural_financeiro_mensal") }}
    )

select *
from financeiro_mensal_raw
