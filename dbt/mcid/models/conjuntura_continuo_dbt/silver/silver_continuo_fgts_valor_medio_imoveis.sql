{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: FGTS — valor médio dos imóveis (R$ e variações),
-- total e Faixa 1. Página 6 (Preços). Dado MANUAL
-- (manual_conjuntura.fgts_valor_medio_imoveis). Validado vs boletim 4T25:
-- DEZ/25 = R$ 245.959, +2,89% mês, +12,14% em 12m.
-- `distinct`: rede de segurança — a tabela manual veio duplicada (insert 2x).
select distinct *
from manual_conjuntura.fgts_valor_medio_imoveis
