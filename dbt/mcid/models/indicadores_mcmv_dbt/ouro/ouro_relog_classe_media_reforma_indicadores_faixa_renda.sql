{{ config(materialized="table") }}

-- OURO do reloginho — composição mensal por faixa de renda, restrita a
-- Classe Média e Reforma Casa Brasil (únicas 2 das 4 frentes novas com
-- `faixa_renda` no contrato de origem), a partir de
-- `prata_relog_classe_media_reforma_resumo_faixa_renda_mes` (change
-- criar-ouro-reloginho-frentes-novas, design.md D4).
--
-- Passagem direta/enriquecida, SEM acumulado (D4): é uma dimensão de filtro
-- para o dashboard (composição por faixa), não uma série de ritmo — ver
-- `ouro_relog_indicadores_frentes_novas` para a série com acumulado.
select
    frente_mcmv,
    mes_referencia,
    faixa_renda,
    n_contratos,
    valor_contratado
from {{ ref("prata_relog_classe_media_reforma_resumo_faixa_renda_mes") }}
order by frente_mcmv, mes_referencia, faixa_renda
