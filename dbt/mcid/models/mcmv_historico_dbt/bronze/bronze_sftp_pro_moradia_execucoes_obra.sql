{{ config(materialized="table") }}

-- BRONZE — série histórica semanal de execução física de obra por contrato
-- Pró-Moradia, família `tab_execucoes_obras` de
-- `staging/sftp/caixa.geavo/GEAVO/MC<aaaammdd>__MCidades_AO_2__tab_execucoes_obras.parquet`,
-- cópia fiel.
--
-- Mesmo padrão de ledger cumulativo de bronze_sftp_pro_moradia_desembolsos:
-- grão contrato × competência de avaliação (`cod_contrato`,
-- `dte_ano_mes_avaliacao`) × snapshot semanal, sem filtro nem dedup aqui — a
-- prata (prata_pro_moradia_historico_execucao_obra) restringe ao universo
-- Pró-Moradia e deduplica por competência.
--
-- ACHADO na implementação (task 6.1): esta família, sozinha, materializa
-- ~21,7M linhas em 38 snapshots com colunas de texto livre bem maiores que
-- as outras 2 famílias GEAVO (`txt_providencias`,
-- `ultima_descricao_motivo_paralisacao_preenchida`) — build local pediu bem
-- mais RAM real que `bronze_sftp_pro_moradia_desembolsos` (61,6M linhas,
-- porém colunas estreitas). Um `post_hook` em 2 lotes sequenciais foi
-- tentado e descartado: dentro da MESMA transação de materialização do dbt,
-- o pico de memória não caiu (o lote 1 não é liberado antes do lote 2 ser
-- lido) — sem ganho real, só complexidade a mais. Exige `DUCKDB_MCID_MEMORY_LIMIT`/
-- `DUCKDB_MCID_CGROUP_MAX` mais folgados que o default do domínio ao rodar
-- localmente numa máquina com RAM apertada (ver README/GOTCHA de memória).
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql).
-- change enriquecer-pro-moradia-execucao-desembolso-historico.
{{ bronze_geavo_semanal('tab_execucoes_obras') }}
