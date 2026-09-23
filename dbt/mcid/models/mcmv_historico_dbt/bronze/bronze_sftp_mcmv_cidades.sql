{{ config(materialized="table") }}

-- BRONZE — série histórica mensal de contratos MCMV Cidades, família
-- `PMCMV_CIDADES_MCID` do SFTP/GEFUS, cópia fiel.
--
-- Glob na staging: staging/sftp/fabrica/GEFUS/**/PMCMV_CIDADES_MCID_*.parquet
-- (snapshots mensais, grão agregado por ente público). Sem dedup nem filtro.
--
-- Fonte COMPLEMENTAR de bronze_shpt_mcmv_cidades_emendas (sharepoint, grão
-- transacional por contrato) — as 2 bronzes não são reconciliadas linha a
-- linha (D2 da change frentes-restantes-mcmv-historico); cada uma materializa
-- isoladamente, sem depender da outra.
--
-- A fonte já traz uma coluna `dt_referencia` própria que colide de nome com a
-- auditoria do domínio — preservada como `dt_referencia_origem_txt`; a
-- auditoria `dt_referencia` é sempre a data do NOME DO ARQUIVO.
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql).
-- change frentes-restantes-mcmv-historico.
{{ bronze_frente_gefus_semanal('PMCMV_CIDADES_MCID') }}
