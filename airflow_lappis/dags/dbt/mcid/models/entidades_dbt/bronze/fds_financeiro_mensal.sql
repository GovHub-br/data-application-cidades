{{ config(materialized="table") }}

-- Bronze: Financeiro Mensal — liberações financeiras FDS (Entidades)
-- Fonte: novo_mcmv_fds_financeiro_mensal
-- Saída: série temporal de liberações com valores tipados por componente
-- OBS:     ic_credito=0 => débito (liberação real, valor negativo no vr_liberado)
--          ic_credito=1 => crédito (devolução, valor positivo)
--          Usar ABS(vr_liberado) para obter o valor real de liberação

with
    financeiro_raw as (
        select
            -- Identificação (APF no financeiro é 6 dígitos, normalizar para 8)
            {{ target.schema }}.normalize_apf(nu_apf) as apf,

            -- Tipo de movimento
            {{ parse_int('co_tipo_movimento') }} as co_tipo_movimento,
            nullif(trim(co_tipo_registro), '') as co_tipo_registro,

            -- Indicador de crédito/débito (0=liberação real, 1=devolução)
            nullif(trim(ic_credito), '') as ic_credito,

            -- Valor total liberado (ATENÇÃO: negativo para ic_credito=0)
            {{ parse_financial_value('vr_liberado') }} as vr_liberado,
            {{ parse_financial_value('vr_movimento') }} as vr_movimento,

            -- Decomposição por componente
            {{ parse_financial_value('vr_pago_obra_empreendimento') }} as vr_pago_obra,
            {{ parse_financial_value('vr_pago_terreno') }} as vr_pago_terreno,
            {{ parse_financial_value('vr_pago_trab_social') }} as vr_pago_trabalho_social,
            {{ parse_financial_value('vr_pago_projeto') }} as vr_pago_projeto,
            {{ parse_financial_value('vr_pago_aporte_suplementacao') }} as vr_pago_aporte,
            {{ parse_financial_value('vr_pago_despesas_incc') }} as vr_pago_incc,
            {{ parse_financial_value('vr_pago_cartorios_legalizacao') }} as vr_pago_legalizacao,
            {{ parse_financial_value('vr_pago_seguranca') }} as vr_pago_seguranca,

            -- Percentual de evolução
            {{ parse_numeric('pc_evolucao', 'numeric(6, 2)') }} as pct_evolucao,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_liberacao_recurso) as dt_liberacao,
            {{ target.schema }}.parse_date_br(dt_remessa) as dt_remessa,
            {{ target.schema }}.parse_date_br(dt_evento) as dt_evento,

            -- Identificador do registro
            nullif(trim(nu_identificador), '') as identificador,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_fds_financeiro_mensal") }}
    )

select *
from financeiro_raw
