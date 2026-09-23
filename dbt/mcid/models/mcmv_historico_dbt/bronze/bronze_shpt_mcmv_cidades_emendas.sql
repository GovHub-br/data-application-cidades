{{ config(materialized="table") }}

-- BRONZE — consolidado transacional de contratos MCMV Cidades (emendas),
-- `staging/sharepoint/novo_mcmv_cidades_emendas.parquet`, cópia fiel.
--
-- Arquivo único (sem glob por data) — o Sharepoint sobrescreve o snapshot
-- corrente a cada ingestão; não há histórico de versões anteriores. 3.877
-- linhas × 61 colunas, `sub_programa = 'PMCMV'` em 3.847/3.877.
--
-- Fonte COMPLEMENTAR de bronze_sftp_mcmv_cidades (GEFUS, grão agregado por
-- ente público) — as 2 bronzes não são reconciliadas linha a linha (D2 da
-- change frentes-restantes-mcmv-historico).
--
-- dt_referencia = data de ingestão do arquivo (`_ingested_at`), já que não há
-- snapshot datado no nome. Corpo em macros/historico/corpos_bronze.sql.
-- change frentes-restantes-mcmv-historico.
{{ bronze_flat_shpt('sharepoint/novo_mcmv_cidades_emendas.parquet') }}
