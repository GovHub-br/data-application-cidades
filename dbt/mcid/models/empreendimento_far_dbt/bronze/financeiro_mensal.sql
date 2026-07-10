{{ config(materialized="table") }}

-- Bronze: Financeiro Mensal — liberações financeiras do empreendimento
-- Fonte: novo_mcmv_far_financeiro_mensal (6.069 registros, 23 colunas text)
-- Saída: série temporal de liberações com valores tipados por componente

with
    financeiro_raw as (
        select
            -- Identificação (APF no financeiro é 6 dígitos, normalizar para 8)
            {{ target.schema }}.normalize_apf(nu_apf) as apf,

            -- Tipo de movimento (1=liberação obra, 5=INCC/ajuste, etc.)
            {{ parse_int('co_tipo_movimento') }} as co_tipo_movimento,
            {{ parse_int('co_tipo_lib_recurso') }} as co_tipo_lib_recurso,
            nullif(trim(co_tipo_registro), '') as co_tipo_registro,

            -- Indicador de crédito/débito
            nullif(trim(ic_credito), '') as ic_credito,

            -- Valor total liberado
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            {{ parse_financial_value('vr_movimento') }} as vr_movimento,

            -- Decomposição por componente
            {{ parse_financial_value('vr_pago_obra_empreendimento') }} as vr_pago_obra,
            {{ parse_financial_value('vr_pago_terreno') }} as vr_pago_terreno,
            {{ parse_financial_value('vr_pago_pts') }} as vr_pago_trabalho_social,
            {{ parse_financial_value('vr_pago_equipamentos_publicos') }} as vr_pago_equipamentos,
            {{ parse_financial_value('vr_pago_aporte_suplementacao') }} as vr_pago_aporte,
            {{ parse_financial_value('vr_pago_despesas_manutencao') }} as vr_pago_manutencao,
            {{ parse_financial_value('vr_pago_despesas_incc') }} as vr_pago_incc,
            {{ parse_financial_value('vr_pago_cartorios_legalizacao') }} as vr_pago_legalizacao,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_liberacao_recurso) as dt_liberacao,
            {{ target.schema }}.parse_date_br(dt_remessa) as dt_remessa,

            -- Identificador do registro
            nullif(trim(no_identificador), '') as identificador,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_far_financeiro_mensal") }}
    )

select *
from financeiro_raw
