{{ config(materialized="table") }}

-- BRONZE — serie executiva historica do MCMV (pre-2024), familia
-- `entrada_bb` (empreendimento BB), copia fiel.
--
-- Uma das 4 tabelas em que a bronze unica da serie executiva foi separada
-- (D5 da change pipeline-bronze-historica-destino-trocavel): cada familia
-- vira uma tabela ESTREITA (so as colunas da propria origem), em vez de uma
-- tabela de 252 colunas esparsas. A uniao das 4 familias, com projecao
-- explicita por braco, vive em silver_mcmv_historico_serie_executiva.
--
-- Glob na staging: staging/dados_historicos/*entrada_bb*.parquet
-- Cada familia tem 2-3 geracoes de schema; a harmonizacao (mapa de colunas)
-- e da silver. Aqui: empilhamento fiel + colunas de auditoria (source_file,
-- dt_referencia, dt_ingest, hash_linha). `dt_referencia` = mes-snapshot:
-- report_date normalizado, com fallback pelo nome do arquivo.
--
-- Corpo e glob vem do mapa de familias (macros/historico/familias.sql).
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`,
-- Postgres atachado em `prod_duckdb`.
{{ bronze_serie_executiva('entrada_bb') }}
