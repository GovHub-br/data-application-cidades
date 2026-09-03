{{ config(materialized="table") }}

-- Bronze: INT 040 — empreendimentos FAR CAIXA PJ (equivalente rico por agente)
-- Fonte: mcmv_staging.int_empreendimentos_int_040_far_caixa_pj (staging/sharepoint)
-- Cópia fiel: colunas preservadas como texto (§6), APF normalizado à parte.
-- NOTA: o nu_apf desta interface está num espaço de identificador diferente do
-- APF do cadastro PJ / consolidado GFAR — não casa por normalize_apf. Materializada
-- para uso futuro; a integração à silver precisa de uma tabela de-para de APF.

with
    int040_raw as (
        select
            {{ normalize_apf('nu_apf') }} as apf,
            nullif(trim(nu_apf), '') as nu_apf_origem,
            * exclude (nu_apf, arquivo_de_origem, criado_em, _source_file, _ingested_at, _source_hash),

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "int_empreendimentos_int_040_far_caixa_pj") }}
    )

select *
from int040_raw
