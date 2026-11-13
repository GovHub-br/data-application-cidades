{{ config(materialized="table") }}

-- Gold: funil de cada proposta selecionada pelas portarias de seleção do Rural (162/2018, 354/2024,
-- 697/2026): seleção → autorização de contratação → contrato. Grão: linha do anexo da portaria de
-- seleção (id_selecao). O vínculo com o contrato e com a autorização é feito na
-- prata_rural_proposta_funil; esta ouro só a expõe para o painel.
--
-- A ouro_rural_selecao_portaria_162 continua igual: para a 162/2018 ela usa só a APF.

select * from {{ ref("prata_rural_proposta_funil") }}
