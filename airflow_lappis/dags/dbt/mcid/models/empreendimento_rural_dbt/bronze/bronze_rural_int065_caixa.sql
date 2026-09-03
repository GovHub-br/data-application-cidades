{{ config(materialized="table") }}

-- Bronze: INT 065 — empreendimentos PNHR CAIXA PJ (equivalente rico por agente)
-- Fonte: mcmv_staging.int_empreendimentos_int_065_pnhr_caixa_pj (staging/sharepoint)
-- Cópia fiel. Contém legado PNHR + Novo MCMV Rural (8.508 linhas). APF de nu_apf
-- (casa parcialmente com o cadastro PJ — ver silver).

with
    int065_raw as (
        select
            {{ normalize_apf('nu_apf') }} as apf,
            nullif(trim(nu_contrato_emprendimento), '') as nu_contrato_empreendimento,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,
            nullif(trim(co_agente_financeiro), '') as agente_financeiro,
            nullif(trim(co_natureza), '') as co_natureza,
            nullif(trim(co_grupo_renda), '') as co_grupo_renda,

            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,

            nullif(trim(no_entidade_organizadora), '') as eo_nome,
            nullif(regexp_replace(trim(nu_cnpj_entidade), '[^0-9]', '', 'g'), '') as eo_cnpj,

            {{ parse_int('qtde_uh_inicial') }} as qt_uh_inicial,
            {{ parse_int('qtde_unidades') }} as qt_uh,
            {{ parse_int('qt_unidades_concluidas') }} as qt_uh_concluidas,
            {{ parse_int('qt_unidades_entregues') }} as qt_uh_entregues,

            {{ parse_hist_numeric('vr_investimento_pnhr') }} as vr_total_investimento,
            {{ parse_hist_numeric('vr_operacao') }} as vr_operacao,
            {{ parse_hist_numeric('vr_edificacao') }} as vr_edificacao,
            {{ parse_hist_numeric('vr_atec') }} as vr_atec,
            {{ parse_hist_numeric('vr_ts') }} as vr_trabalho_social,
            {{ parse_hist_numeric('vr_contrapartida') }} as vr_total_contrapartidas,
            {{ parse_hist_numeric('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_hist_numeric('vr_subsidio_fgts') }} as vr_subsidio_fgts,
            {{ parse_hist_numeric('vr_liberado') }} as vr_liberado,

            {{ parse_numeric('pc_obra_realizado', 'numeric(6, 2)') }} as pct_obra_realizada,
            nullif(trim(no_situacao_obra), '') as situacao_obra,
            {{ parse_int('pz_construcao') }} as prazo_construcao,

            {{ parse_date_br('dt_contrato') }} as dt_contratacao,
            {{ parse_date_br('dt_ultima_liberacao') }} as dt_ultima_liberacao,
            {{ parse_date_br('dt_efetiva_conclusao') }} as dt_conclusao_obra,
            {{ parse_date_br('dt_movimento') }} as dt_movimento,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "int_empreendimentos_int_065_pnhr_caixa_pj") }}
    )

select *
from int065_raw
