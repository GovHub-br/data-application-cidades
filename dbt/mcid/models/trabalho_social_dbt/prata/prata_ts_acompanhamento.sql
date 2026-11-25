{{ config(materialized="table") }}

-- Prata: acompanhamento do Trabalho Social por contrato na remessa de jul/2026, nas três linhas
-- (FAR, FDS/Entidades e Rural/PNHR) e nos dois agentes financeiros (CAIXA e BB), num formato único.
-- Grão: linha × agente financeiro × contrato. Chave: id_ts.
--
-- Fontes (bronzes de jul/2026, cópia fiel das planilhas):
--   FAR CAIXA   bronze_shpt_mcmv_far_ts_caixa_202607
--   FAR BB      bronze_ts_trabalho-social-2026-07__ts_af_bb_far_07_26
--   FDS CAIXA   bronze_shpt_mcmv_fds_ts_caixa_202607
--   RURAL CAIXA bronze_shpt_mcmv_rural_ts_caixa_202607
--   RURAL BB    bronze_shpt_mcmv_rural_ts_caixa_bb_202607 (apesar do nome, é a aba do BB)
--
-- Portaria de TS: aqui fica só a INFORMADA na planilha (portaria_ts_informada) e a que estava em
-- vigor na data de contratação (portaria_ts_vigente_contratacao, seed ts_portarias). A portaria
-- final, com o histórico das remessas, está em prata_ts_portaria_contrato.
--
-- Diferenças de layout tratadas aqui:
--   * BB grava % de execução do TS e % de obra entre 0 e 1; a CAIXA, entre 0 e 100. Tudo sai 0-100.
--   * BB grava a portaria só com o número ("21"); a CAIXA, "21-2014" (macro portaria_ts_normalizada).
--   * A planilha pode repetir o contrato: fica a linha de maior valor global de TS e
--     qt_linhas_origem conta as repetições.
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_acompanhamento).

{{ corpo_prata_ts_acompanhamento() }}
