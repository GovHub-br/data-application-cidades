{{ config(materialized="table") }}

-- Gold: atos que regem o Pró-Moradia (resolução do CCFGTS, instruções normativas do MCID e a seleção por
-- carta-consulta). Grão: ato. Não há portaria de seleção com lista: o elo contrato → ato está na
-- ouro_pro_moradia_contrato_selecao (texto cod_ident_externo do Canal FGTS).

select * from {{ ref("prata_pro_moradia_portarias") }}
