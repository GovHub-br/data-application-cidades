{{ config(materialized="table") }}

-- Bronze: Eventos de mudanca de fase FDS (Entidades)
-- Fonte: fds_cadastro_pj onde ic_mudanca_fase = true.
-- OBS: no snapshot atual, ic_mudanca_fase = false para todos os 343 registros
-- (mudancas de fase ja resolvidas). O vinculo operacional hoje vem do INT059
-- (nu_apf_nao_obra). Este modelo detecta mudancas futuras por ingestao.
select
    apf,
    {{ normalize_apf('apf_mudanca_fase') }} as apf_mudanca_fase,
    dt_movimento,
    source_file,
    dt_ingest,
    hash_linha,
    dt_referencia
from {{ ref("bronze_fds_cadastro_pj") }}
where ic_mudanca_fase = true
