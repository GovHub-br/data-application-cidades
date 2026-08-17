{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/bronze_mcid_contratacoes.parquet',
    options={'compression': 'snappy'}
) }}

-- Bronze tipada sobre uma AMOSTRA REAL do lake (96 linhas de contratações MCMV).
-- Exercita os UDFs portados sobre a semântica real do MCid:
--   APF                 -> poc_normalize_apf   (porte de f_normalize_apf.sql)
--   Data de Contratação -> poc_parse_date_br   (porte de f_parse_date_br.sql)
--   Valor Contratado    -> poc_parse_valor_br  (porte de parse_financial_value.sql)
--
-- Duas diferenças em relação ao CSV, ambas descobertas na prática:
--   1. read_xlsx não tem normalize_names: os nomes vêm crus, com acento e espaço.
--   2. a source NÃO usa all_varchar (ver sources.yml): numa planilha as colunas numéricas
--      e de data já vêm tipadas, então aqui os CASTs partem de DOUBLE/DATE, não de texto.
--      APF chega como DOUBLE e precisa virar texto sem notação científica antes de
--      normalizar — é por isso que há um cast intermediário para BIGINT.

select
    nullif(trim("Agente Financeiro"), '')                        as agente_financeiro,
    poc_normalize_apf(cast(cast("APF" as bigint) as varchar))    as apf,
    nullif(trim("UF"), '')                                       as uf,
    nullif(trim("Município"), '')                                as municipio,
    try_cast("Código IBGE do Município" as integer)              as codigo_ibge_municipio,
    nullif(trim("Nome Empreendimento"), '')                      as nome_empreendimento,
    nullif(trim("Modalidade"), '')                               as modalidade,
    "Data de Contratação"                                        as data_de_contratacao,
    try_cast("Valor Contratado" as decimal(15, 2))               as valor_contratado,
    try_cast("UH Contratadas" as integer)                        as uh_contratadas,
    try_cast(nullif(trim("Data Movimento"), '') as timestamp)    as data_movimento,
    'raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250730.xlsx' as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)                as _ingested_at

from {{ source('poc_raw', 'mcid_contratacoes') }}
