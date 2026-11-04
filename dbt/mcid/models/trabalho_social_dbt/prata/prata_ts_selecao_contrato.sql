{{ config(materialized="table") }}

-- Prata: portaria de SELEÇÃO de onde saiu cada contrato (FAR e FDS; Rural pendente). Não é a portaria de
-- Trabalho Social: diz em que ciclo de seleção o empreendimento entrou. Grão: linha × APF.
-- Chave: id_selecao_contrato.
--
-- Fontes e método do vínculo:
--   FAR    seeds.far_portaria_selecao_apf (fonte seeds_manuais): já vem por APF            -> 'Exata'
--   FDS    bronze_seed_manual_fds_portaria_selecao_698_2026 + Cad PJ FDS (bronze, número da proposta) -> 'Exata'
--          (em out/2026 nenhuma das 393 propostas da 698/2026 está contratada no Cad PJ)
--   RURAL  pendente: depende da prata_rural_selecao_contrato (branch feat/dbt_pro_moradia_fnhis)
-- Quando o mesmo APF cai em mais de uma seleção, fica a de maior confiança e, no empate, a portaria
-- mais recente; qt_selecoes_candidatas mostra quantas havia.
--
-- O SQL está em macros/corpos/corpos_trabalho_social.sql (corpo_prata_ts_selecao_contrato).

{{ corpo_prata_ts_selecao_contrato() }}
