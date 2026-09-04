{{ config(materialized="table") }}

-- Bronze: Cadastro PJ Rural (PNHR / MCMV Rural) — empreendimento contratado
-- Fonte: mcmv_staging.novo_mcmv_rural_cad_pj_mensal (staging/sharepoint via MinIO/DuckDB)
-- Cópia fiel: só tipagem/normalização técnica, sem dedup.
-- Grão: 1 linha por empreendimento (127 no snapshot atual — recorte Novo MCMV Rural).

with
    cad_pj_raw as (
        select
            {{ normalize_apf('nu_apf_com_dv') }} as apf,
            nullif(trim(nu_contrato_empreend), '') as nu_contrato_empreendimento,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,

            -- Entidade Organizadora
            nullif(trim(no_nome_eo), '') as eo_nome,
            nullif(regexp_replace(trim(co_cnpj_eo), '[^0-9]', '', 'g'), '') as eo_cnpj,
            case when trim(ic_eo_substituida) = 'S' then true else false end as ic_substituicao_eo,
            nullif(trim(no_nome_novo_eo), '') as eo_substituta_nome,
            nullif(regexp_replace(trim(co_cnpj_novo_eo), '[^0-9]', '', 'g'), '') as eo_substituta_cnpj,

            nullif(trim(co_agente_finan), '') as agente_financeiro,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(nu_ibge_empreend), '') as cod_ibge,

            -- Modalidade / regime
            {{ parse_int('nu_modalidade') }} as co_modalidade,

            -- UHs
            {{ parse_int('qt_uh_selecionadas') }} as qt_uh_selecionadas,
            {{ parse_int('qt_uh_contratadas') }} as qt_uh_contratadas,
            {{ parse_int('qt_uh_concluidas') }} as qt_uh_concluidas,

            -- Valores
            {{ parse_hist_numeric('vr_investimento_total') }} as vr_total_investimento,
            {{ parse_hist_numeric('vr_obra') }} as vr_obra,
            {{ parse_hist_numeric('vr_atec') }} as vr_atec,
            {{ parse_hist_numeric('vr_ts') }} as vr_trabalho_social,
            {{ parse_hist_numeric('vr_custo_indireto') }} as vr_custo_indireto,
            {{ parse_hist_numeric('vr_contrapartida') }} as vr_total_contrapartidas,
            {{ parse_hist_numeric('vr_emprestimo') }} as vr_emprestimo,
            {{ parse_hist_numeric('vr_liberado') }} as vr_liberado,
            {{ parse_hist_numeric('vr_aporte') }} as vr_aporte,
            {{ parse_hist_numeric('vr_suplementacao') }} as vr_suplementacao,
            {{ parse_int('qt_cisterna') }} as qt_cisterna,
            {{ parse_int('qt_efluente') }} as qt_efluente,

            -- Execução física
            {{ parse_numeric('pc_obra_realizada', 'numeric(6, 2)') }} as pct_obra_realizada,
            {{ parse_int('co_situacao_obra') }} as co_situacao_obra,
            {{ parse_int('pz_construcao') }} as prazo_construcao,

            -- Datas
            {{ parse_date_br('dt_contratacao') }} as dt_contratacao,
            {{ parse_date_br('dt_retomada') }} as dt_retomada,
            {{ parse_date_br('dt_ult_liberacao') }} as dt_ultima_liberacao,
            {{ parse_date_br('dt_conclusao') }} as dt_conclusao_obra,

            -- Colunas técnicas (padrão medalhão §6)
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_rural_cad_pj_mensal") }}
    )

select *
from cad_pj_raw
