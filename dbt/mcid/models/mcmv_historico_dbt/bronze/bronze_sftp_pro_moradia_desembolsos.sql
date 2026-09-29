{{ config(materialized="table") }}

-- BRONZE — série histórica semanal de desembolsos do FGTS por contrato
-- Pró-Moradia, família `tab_desembolsos_fgts` de
-- `staging/sftp/caixa.geavo/GEAVO/MC<aaaammdd>__MCidades_AO_2__tab_desembolsos_fgts.parquet`,
-- cópia fiel.
--
-- Fonte é um LEDGER CUMULATIVO, não um delta semanal: cada snapshot
-- reexporta o histórico completo até aquela data (o mais antigo dos 38
-- snapshots já traz `dte_ano` de 1997 a 2025). Grão da fonte: contrato ×
-- competência (`cod_contrato`, `dte_ano`, `dte_mes_ref`) × snapshot semanal
-- — sem filtro nem dedup aqui (convenção do domínio); a prata
-- (prata_pro_moradia_historico_desembolso_mensal) restringe ao universo
-- Pró-Moradia e deduplica por competência.
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql).
-- change enriquecer-pro-moradia-execucao-desembolso-historico.
{{ bronze_geavo_semanal('tab_desembolsos_fgts') }}
