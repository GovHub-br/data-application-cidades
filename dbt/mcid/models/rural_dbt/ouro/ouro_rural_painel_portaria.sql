{{ config(materialized="table") }}

-- Gold: dataset da aba Rural com o filtro único de portaria. É a ouro_rural_painel_operacao (uma linha
-- por APF) desdobrada em três linhas por APF, uma por tipo de portaria:
--   Seleção                     — a portaria que selecionou a proposta do contrato (162/2018, 354/2024, 697/2026);
--   Autorização de contratação  — a portaria que autorizou contratar a proposta (1.133/2024, 1.346/2024…);
--   Trabalho Social             — a portaria de TS adotada na operação (21/2014, 464/2018, 75/2025).
-- Grão: APF × tipo_portaria (sempre 3 linhas por APF). Lê só prata.
--
-- Regra de uso no painel: escolha UM tipo de portaria (o filtro "Tipo de portaria" é obrigatório e de
-- seleção única). Com um tipo escolhido, cada APF aparece uma vez, e contagens e somas valem; sem esse
-- filtro tudo seria contado três vezes. APF sem portaria do tipo escolhido entra com um rótulo que diz
-- por quê (`portaria`), para os totais baterem com a ouro_rural_painel_operacao.

with op as (select * from {{ ref("prata_rural_vinculo_operacao") }})

select
    md5(op.apf || '|' || tp.tipo_portaria) as id_apf_tipo_portaria,
    tp.tipo_portaria,
    tp.portaria,
    tp.situacao_portaria,
    tp.ic_tem_portaria,
    op.*
from op
{{ rural_portaria_por_tipo("op") }}
