{{ config(materialized="table") }}

-- Bronze: INT 054 — empreendimentos FAR BB PJ (equivalente rico por agente)
-- Fonte: mcmv_staging.int_empreendimentos_int_054_far_bb_pj (staging/sharepoint)
-- Cópia fiel: colunas preservadas como texto (§6), APF normalizado à parte.
-- NOTA: mesmo caveat de espaço de identificador do bronze_far_int040_caixa.
with
    int054_raw as (
        select
            {{ normalize_apf('nu_apf') }} as apf,
            nullif(trim(nu_apf), '') as nu_apf_origem,
            * exclude (
                nu_apf,
                arquivo_de_origem,
                criado_em,
                _source_file,
                _ingested_at,
                _source_hash
            ),

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "int_empreendimentos_int_054_far_bb_pj") }}
    )

select *
from int054_raw
