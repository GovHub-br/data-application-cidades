{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: INCC-M (FGV).
-- A bronze espelha o parquet de staging; aqui é só tipagem.
--
-- Ausente vem em DOIS formatos na fonte: string vazia e `...` (o marcador
-- que FGV e IBGE usam). O `...` aparece em `var_12_meses` nos 12 primeiros
-- meses da série (1994-1995), onde variação em 12 meses ainda não existe —
-- sem tratar, o cast quebra com
-- 'Could not convert string "..." to DECIMAL'.

select
    mes::date                                          as mes,
    nullif(nullif(indice::text, ''), '...')::numeric        as indice,
    nullif(nullif(var_mes::text, ''), '...')::numeric       as var_mes,
    nullif(nullif(var_ano::text, ''), '...')::numeric       as var_ano,
    nullif(nullif(var_12_meses::text, ''), '...')::numeric  as var_12_meses
from {{ ref('bnz_fgv_incc_m') }}
