{{ config(materialized="table") }}

-- Prata: vínculo de cada proposta selecionada (todas as portarias de seleção do Rural) com o
-- contrato que saiu dela. Grão: linha do anexo da portaria (id_selecao).
--
-- O SQL está em macros/corpos/corpos_rural.sql (corpo_prata_rural_selecao_contrato), porque a
-- prata do Trabalho Social (prata_ts_selecao_contrato) usa a mesma regra e prata não lê prata.

{{ corpo_prata_rural_selecao_contrato() }}
