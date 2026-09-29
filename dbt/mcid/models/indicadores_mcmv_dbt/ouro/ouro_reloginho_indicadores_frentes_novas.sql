{{ config(materialized="table") }}

-- OURO do reloginho: série mensal por frente para Classe Média, Reforma Casa
-- Brasil, MCMV Cidades e Pró-Moradia (change criar-ouro-reloginho-frentes-
-- novas, design.md D2). Lê `prata_reloginho_frentes_novas_resumo_mes`
-- (inalterada) — dedup e agregação mensal já feitas na prata, este modelo só
-- adiciona o acumulado corrido.
--
-- DIFERENÇA ESTRUTURAL vs. `ouro_reloginho_indicadores*` (FAR/Entidades/
-- Rural): lá a fonte SNH já é ESTOQUE (snapshot acumulado por mês); aqui
-- `n_contratos`/`valor_contratado` são FLUXO (contratos cuja dt_contratacao
-- cai naquele mês). Por isso o acumulado corrido é CALCULADO aqui via
-- `sum(...) over (partition by frente_mcmv order by mes_referencia)`, e não
-- copiado de um estoque já pronto na fonte. NÃO confundir `n_contratos`
-- (fluxo do mês) com `n_contratos_acumulado` (soma corrida) — ver design.md
-- D2.
with

    base as (select * from {{ ref("prata_reloginho_frentes_novas_resumo_mes") }})

select
    frente_mcmv,
    mes_referencia,
    n_contratos,
    valor_contratado,
    sum(n_contratos) over (
        partition by frente_mcmv order by mes_referencia
    ) as n_contratos_acumulado,
    sum(valor_contratado) over (
        partition by frente_mcmv order by mes_referencia
    ) as valor_contratado_acumulado,
    count(*) over (
        partition by frente_mcmv order by mes_referencia
    ) as n_meses_observados
from base
order by frente_mcmv, mes_referencia
