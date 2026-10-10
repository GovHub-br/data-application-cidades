{{ config(materialized='table') }}

-- Prata do conjuntura: Índice FipeZap de locação (FIPE).
-- Full-refresh: cada run reconstrói a tabela a partir do `latest/` da staging.
--
-- A staging guarda a aba `Índice FipeZAP` como veio, com o cabeçalho da linha 4:
-- `Data` e uma coluna `Total` por série, numeradas pela ordem na planilha
-- (`Total`, `Total_2`…). A locação residencial é `Total_5` (número-índice),
-- `Total_6` (variação mensal) e `Total_7` (variação em 12 meses). A escolha é por
-- posição; o teste `conjuntura_fipezap_colunas_coerentes` acusa se a FIPE mexer
-- nas colunas. `.` marca ausente; o rodapé vazio da planilha fica de fora.

select
    cast("Data" as timestamp)::date                         as data_referencia,
    nullif(nullif("Total_5", ''), '.')::numeric             as imoveis_residenciais_locacao_numero_indice_total,
    nullif(nullif("Total_6", ''), '.')::numeric             as imoveis_residenciais_locacao_var_mensal_total,
    nullif(nullif("Total_7", ''), '.')::numeric             as imoveis_residenciais_locacao_var_ano_total,
    {{ lake_dt_ingest() }}                                  as dt_ingest
from {{ ref('bronze_fipezap_locacao') }}
where "Data" is not null
