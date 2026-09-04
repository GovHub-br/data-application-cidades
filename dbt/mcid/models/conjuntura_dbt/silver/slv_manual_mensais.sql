{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: TODOS os dados manuais MENSAIS do boletim,
-- da planilha oficial (boletim.xlsx, aba "Dados Mensais"), carregada em
-- conjuntura.bnz_manual_dados_mensais. Cobre CAGED, PNAD rendimento, índices de
-- preços, crédito Bacen (estoques/concessões), SBPE por banco, financiamento
-- PF MCMV por faixa, valor médio FGTS, etc.

select *
from conjuntura.bnz_manual_dados_mensais
where periodo is not null
