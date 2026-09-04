{{ config(materialized="table") }}

-- Bronze: INT 057 — empreendimentos PNHR BB PJ (equivalente rico por agente)
-- Fonte: mcmv_staging.int_empreendimentos_int_057_pnhr_bb_pj (staging/sharepoint)
-- Cópia fiel. NÃO tem coluna de APF — chave é nu_contrato_empreendimento.
-- Não casa com o cadastro PJ Rural pelo contrato no snapshot atual; materializada
-- como fonte standalone para linhagem/uso futuro.
with
    int057_raw as (
        select
            nullif(trim(nu_contrato_empreendimento), '') as nu_contrato_empreendimento,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,
            nullif(trim(co_agente_financeiro), '') as agente_financeiro,
            nullif(trim(natureza_contrato), '') as natureza_contrato,
            nullif(trim(co_grupo_renda), '') as co_grupo_renda,

            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,

            nullif(trim(no_entidade_organizadora), '') as eo_nome,
            nullif(
                regexp_replace(trim(nu_cnpj_entidade), '[^0-9]', '', 'g'), ''
            ) as eo_cnpj,

            {{ parse_int('qt_unidades') }} as qt_uh,
            {{ parse_int('qt_unidades_concluidas') }} as qt_uh_concluidas,
            {{ parse_int('qt_unidades_entregues') }} as qt_uh_entregues,

            {{ parse_hist_numeric('vr_investimento') }} as vr_total_investimento,
            {{ parse_hist_numeric('vr_operacao') }} as vr_operacao,
            {{ parse_hist_numeric('vr_edificacao') }} as vr_edificacao,
            {{ parse_hist_numeric('vr_atec') }} as vr_atec,
            {{ parse_hist_numeric('vr_ts') }} as vr_trabalho_social,
            {{ parse_hist_numeric('vr_contrapartida') }} as vr_total_contrapartidas,
            {{ parse_hist_numeric('vr_liberado') }} as vr_liberado,

            {{ parse_numeric('pc_execucao_fisica_obra', 'numeric(6, 2)') }}
            as pct_execucao_fisica,
            {{ parse_numeric('pc_execucao_financeira_obra', 'numeric(6, 2)') }}
            as pct_execucao_financeira,
            nullif(trim(no_situacao_obra), '') as situacao_obra,
            {{ parse_int('pz_obra') }} as prazo_obra,

            {{ parse_date_br('dt_contrato') }} as dt_contratacao,
            {{ parse_date_br('dt_ultima_liberacao') }} as dt_ultima_liberacao,
            {{ parse_date_br('dt_efetiva_conclusao') }} as dt_conclusao_obra,
            {{ parse_date_br('dt_movimento') }} as dt_movimento,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "int_empreendimentos_int_057_pnhr_bb_pj") }}
    )

select *
from int057_raw
