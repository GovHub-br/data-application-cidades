{{ config(materialized="table") }}

-- Gold: Matriz pergunta × dado das três linhas acompanhadas com o MCID (Rural, FNHIS Sub-50,
-- Pró-Moradia): o que cada pergunta da oficina consegue responder hoje, em qual ouro, o que falta
-- e qual a limitação. Fonte: seed perguntas_mcid/cobertura_perguntas_mcid (curada a partir das
-- explorações de 2026-09/10). Mesmo formato das tabelas de cobertura de outros produtos.

select
    programa,
    ordem,
    origem_pergunta,
    pergunta,
    situacao_dado,
    case situacao_dado
        when 'respondivel' then 'Respondível'
        when 'parcial' then 'Parcial'
        when 'lacuna_de_fonte' then 'Lacuna de fonte'
        else situacao_dado
    end as situacao_dado_rotulo,
    resposta_disponivel,
    nullif(modelo_ouro, '') as modelo_ouro,
    nullif(fonte_necessaria, '') as fonte_necessaria,
    nullif(limitacao, '') as limitacao
from {{ ref("cobertura_perguntas_mcid") }}
