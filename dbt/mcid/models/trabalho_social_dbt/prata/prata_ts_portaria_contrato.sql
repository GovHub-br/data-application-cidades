{{ config(materialized="table") }}

-- Prata: a portaria de Trabalho Social de cada contrato, resolvida, e de onde ela veio.
-- Grão: linha × agente financeiro × contrato da remessa de jul/2026. Chave: id_ts.
-- Fontes (embutidas como corpos, a prata só lê bronze): corpo_prata_ts_acompanhamento (jul/2026),
-- corpo_prata_ts_portaria_remessa (remessas desde jan/2025),
-- bronze_seed_ts_portarias (vigência e se os marcos já foram transcritos) e corpo_prata_ts_selecao_contrato
-- (portaria de SELEÇÃO de onde o contrato saiu, com o método e a confiança do vínculo).
--
-- Ordem de preferência (coluna origem_portaria_ts):
--   1. 'informada'         -> a planilha de jul/2026 traz a portaria;
--   2. 'remessa anterior'  -> jul/2026 veio vazia, mas uma remessa anterior trouxe
--                             (vale a mais recente que trouxe);
--   3. 'sem portaria'      -> nenhuma remessa trouxe. portaria_ts fica nula.
--
-- A portaria em vigor na data de contratação (portaria_ts_vigente_contratacao) fica só como
-- referência e NÃO preenche portaria_ts. Conferido na remessa de jul/2026 (CAIXA, contratos com
-- portaria informada): ela acerta 5,6% no FAR contratado antes de 2014, 11,5% no FAR de 2018 a
-- 2025, 55% no FDS e 76% no Rural contratados depois de 05/02/2025. Os contratos antigos migraram
-- para normas mais novas (art. 2º da 21/2014, art. 3º da 464/2018, art. 36 da 75/2025).
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_portaria_contrato).

{{ corpo_prata_ts_portaria_contrato() }}
