{{ config(materialized="table") }}

-- Prata: matriz pergunta × dado das três linhas acompanhadas com o MCID (Rural, FNHIS Sub-50,
-- Pró-Moradia), uma linha por pergunta. Fonte: bronze_seed_cobertura_perguntas_mcid.
-- Só fixa as colunas; rótulos e limpeza ficam na ouro (ouro_mcid_cobertura_perguntas).

select
    programa,
    ordem,
    origem_pergunta,
    pergunta,
    situacao_dado,
    resposta_disponivel,
    modelo_ouro,
    fonte_necessaria,
    limitacao
from {{ ref("bronze_seed_cobertura_perguntas_mcid") }}
