{{ config(materialized="table") }}

-- BRONZE — tabela de domínio de linhas de crédito do Canal FGTS (pequena),
-- `staging/sharepoint/fgts_canal_tdom_ao_1_linha.parquet`, cópia fiel.
--
-- Resolve `cod_linha` para texto (ex.: `'26'` -> `'HAB / PRO-MORADIA'`) — usada
-- pela prata de Pró-Moradia, mas não é exclusiva dela: qualquer prata futura
-- que precise decodificar `cod_linha` do Canal FGTS pode reusar esta bronze.
--
-- dt_referencia = data de ingestão do arquivo (`_ingested_at`). Corpo em
-- macros/historico/corpos_bronze.sql.
-- change frentes-restantes-mcmv-historico.
{{ bronze_flat_shpt('sharepoint/fgts_canal_tdom_ao_1_linha.parquet') }}
