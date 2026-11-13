{{ config(materialized="table") }}

-- Gold: curva S de referência do Pró-Moradia por TEMPO DE CONTRATO (meses desde a assinatura): para cada idade,
-- a distribuição do % físico realizado (medições mensais do Canal FGTS) e do % financeiro desembolsado de todos os
-- contratos com medição naquela idade. A mediana é a curva padrão; p25–p75 é a metade central. Grão: idade_meses.
-- Lê só prata (macros/curva_s_linhas.sql).

select * from ({{ curva_s_referencia_de(pro_moradia_curva_s_obra_mes()) }}) r
