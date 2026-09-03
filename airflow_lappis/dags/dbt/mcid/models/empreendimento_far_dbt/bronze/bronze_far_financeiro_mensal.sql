{{ config(materialized="table") }}

-- Bronze: Financeiro Mensal — liberações financeiras do empreendimento (frente FAR)
-- Fonte: mcmv_staging.novo_mcmv_far_financeiro_mensal (staging/sharepoint via MinIO/DuckDB)
-- Cópia fiel: só tipagem/normalização técnica, sem dedup. APF em 6 dígitos → normalize_apf.

with
    financeiro_raw as (
        select
            -- Identificação (APF no financeiro é 6 dígitos, normalizar para 8)
            {{ normalize_apf('nu_apf') }} as apf,

            -- Tipo de movimento (1=liberação obra, 5=INCC/ajuste, etc.)
            {{ parse_int('co_tipo_movimento') }} as co_tipo_movimento,
            {{ parse_int('co_tipo_lib_recurso') }} as co_tipo_lib_recurso,
            nullif(trim(co_tipo_registro), '') as co_tipo_registro,

            -- Indicador de crédito/débito
            nullif(trim(ic_credito), '') as ic_credito,

            -- Valor total liberado
            {{ parse_hist_numeric('vr_liberado') }} as vr_liberado,
            {{ parse_hist_numeric('vr_movimento') }} as vr_movimento,

            -- Decomposição por componente
            {{ parse_hist_numeric('vr_pago_obra_empreendimento') }} as vr_pago_obra,
            {{ parse_hist_numeric('vr_pago_terreno') }} as vr_pago_terreno,
            {{ parse_hist_numeric('vr_pago_pts') }} as vr_pago_trabalho_social,
            {{ parse_hist_numeric('vr_pago_equipamentos_publicos') }} as vr_pago_equipamentos,
            {{ parse_hist_numeric('vr_pago_aporte_suplementacao') }} as vr_pago_aporte,
            {{ parse_hist_numeric('vr_pago_despesas_manutencao') }} as vr_pago_manutencao,
            {{ parse_hist_numeric('vr_pago_despesas_incc') }} as vr_pago_incc,
            {{ parse_hist_numeric('vr_pago_cartorios_legalizacao') }} as vr_pago_legalizacao,

            -- Datas
            {{ parse_date_br('dt_movimento') }} as dt_movimento,
            {{ parse_date_br('dt_liberacao_recurso') }} as dt_liberacao,
            {{ parse_date_br('dt_remessa') }} as dt_remessa,

            -- Identificador do registro
            nullif(trim(no_identificador), '') as identificador,

            -- Colunas técnicas (padrão medalhão §6)
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_far_financeiro_mensal") }}
    )

select *
from financeiro_raw
