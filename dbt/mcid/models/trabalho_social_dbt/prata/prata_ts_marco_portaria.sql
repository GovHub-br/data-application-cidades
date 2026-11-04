{{ config(materialized="table") }}

-- Prata: régua de marcos temporais do Trabalho Social por portaria e linha. É a regra que o painel
-- usa para dizer o que já deveria estar pronto em cada contrato (junta por portaria_ts e linha com
-- prata_ts_portaria_contrato). Grão: marco. Chave: id_marco.
-- Fontes: seeds ts_marcos_portaria_464_2018 (Anexos III, IV e V) e ts_marcos_portaria_75_2025
-- (Anexo II). As portarias 21/2014 e 168/2013 ainda não têm régua: os marcos estão no Manual de
-- Instruções do TS (21/2014) e no Anexo V da 168/2013, que não foram transcritos.
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_marco_portaria).

{{ corpo_prata_ts_marco_portaria() }}
