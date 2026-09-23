{{ config(materialized="table") }}

-- BRONZE — propostas FNHIS/SUB50 selecionadas,
-- `staging/sharepoint/novo_mcmv_fnhis_sub_50_propostas_selecionadas.parquet`,
-- cópia fiel. 1.207 linhas.
--
-- Materializa isoladamente de bronze_shpt_sub50_propostas_apresentadas — a
-- prata (prata_sub50_historico_proposta) une as duas com `status_proposta`
-- discriminando apresentada/selecionada.
--
-- dt_referencia = data de ingestão do arquivo (`_ingested_at`). Contrato de
-- colunas de referência:
-- docs/entregas/issue-119-ajuste-frentes-faltantes.md. Corpo em
-- macros/historico/corpos_bronze.sql.
-- change frentes-restantes-mcmv-historico.
{{ bronze_flat_shpt('sharepoint/novo_mcmv_fnhis_sub_50_propostas_selecionadas.parquet') }}
