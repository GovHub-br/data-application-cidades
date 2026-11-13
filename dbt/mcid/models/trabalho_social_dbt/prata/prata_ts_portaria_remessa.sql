{{ config(materialized="table") }}

-- Prata: portaria de TS informada para cada contrato em CADA remessa guardada na staging
-- (jan/2025 a jul/2026). Grão: linha × agente financeiro × contrato × arquivo. Chave: id_ts_remessa.
-- Fonte: bronze_shpt_ts_remessas_historico (todas as planilhas da pasta Trabalho Social).
--
-- Linha, agente e competência saem do nome do arquivo:
--   MCMV_FAR_TS_CAIXA_BB_ 202607__ts_af_caixa_far_07_26  -> FAR, CAIXA, 2026-07-31
--   BASE_PNHR_TS_01_2025__pnhr_af_bb_31_01_2025          -> RURAL, BB, 2025-01-31
-- Ficam de fora as abas auxiliares (planilha1, planilha2) e a base de datas de 30/80/100% de obra.
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_portaria_remessa).

{{ corpo_prata_ts_portaria_remessa() }}
