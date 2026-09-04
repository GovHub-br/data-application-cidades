{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: SBPE — financiamentos (construção/aquisição),
-- desagregação por banco e captação líquida da poupança. Páginas 4 e 5.
-- Dado MANUAL (conjuntura.bnz_manual_sbpe_financiamentos_aquisicao_bancos).
-- Validado vs boletim 4T25: SBPE Const 2025 = 132.859 UH / R$ 37,99 bi;
-- captação poupança 12m/25 = -63,0 bi.

select *
from conjuntura.bnz_manual_sbpe_financiamentos_aquisicao_bancos
