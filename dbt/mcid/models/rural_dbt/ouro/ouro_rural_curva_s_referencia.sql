{{ config(materialized="table") }}

-- Gold: curva S de referência do Novo MCMV Rural por TEMPO DE CONTRATO (meses desde a contratação).
-- Para cada idade, a distribuição do % físico realizado e do % financeiro de todos os empreendimentos que
-- tiveram retrato naquela idade (MONIT_MOV_OBRA, dez/2025 em diante): p10, p25, mediana, p75 e p90.
-- A mediana é a "curva padrão"; a faixa p25–p75 é onde está a metade central dos empreendimentos.
-- Grão: idade_meses. Lê só prata (macro rural_curva_s_referencia).
--
-- Por que por idade e não por mês do calendário: a carteira mistura contratos de 2024 e de 2025; no mesmo
-- mês, um contrato de 3 meses e um de 20 meses não são comparáveis. Comparar na mesma idade é o que mostra
-- quem anda mais devagar que os outros.

select * from ({{ rural_curva_s_referencia() }}) r
