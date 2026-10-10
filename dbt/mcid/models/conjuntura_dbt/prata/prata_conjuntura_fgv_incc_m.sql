{{ config(materialized='table') }}

-- Prata do conjuntura: INCC-M (FGV).
-- A bronze espelha o parquet de staging; aqui é só tipagem.
--
-- Ausente vem em DOIS formatos na fonte: string vazia e `...` (o marcador
-- que FGV e IBGE usam). O `...` aparece em `var_12_meses` nos 12 primeiros
-- meses da série (1994-1995), onde variação em 12 meses ainda não existe —
-- sem tratar, o cast quebra com
-- 'Could not convert string "..." to DECIMAL'.

--
-- A staging guarda o cabeçalho da planilha como veio (linha 3 do xlsx): mês e
-- índice ficam sem nome (column_1, column_2) e as variações são `No mês`,
-- `No ano` e `12 meses`. O mês chega como texto ISO da célula de data. A última
-- linha da planilha é o rodapé "Fonte: FGV", descartado aqui.

select
    column_1::date                                          as mes,
    nullif(nullif(column_2, ''), '...')::numeric            as indice,
    nullif(nullif("No mês", ''), '...')::numeric            as var_mes,
    nullif(nullif("No ano", ''), '...')::numeric            as var_ano,
    nullif(nullif("12 meses", ''), '...')::numeric          as var_12_meses
from {{ ref('bronze_fgv_incc_m') }}
where column_1 is not null
  and column_1 not ilike 'fonte%'
