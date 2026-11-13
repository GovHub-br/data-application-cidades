{{ config(materialized="table") }}

-- Gold: Operações do Rural com seus vínculos (portaria de seleção, portaria de Trabalho Social,
-- contrato/empreendimento e entidade organizadora), execução física e financeira e marcas de
-- qualidade. Dataset principal da aba Rural do painel do MCID. Grão: APF.
-- Regras e fórmulas: ver prata_rural_vinculo_operacao.

select * from {{ ref("prata_rural_vinculo_operacao") }}
