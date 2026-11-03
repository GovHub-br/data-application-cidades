{{ config(materialized="table") }}

-- Ouro: matriz pergunta × dado do painel de perguntas do FAR — o que cada pergunta da
-- oficina com o MCID consegue responder hoje, em qual ouro, o que falta e qual a limitação.
select
    bloco,
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
    nullif(resposta_disponivel, '') as resposta_disponivel,
    nullif(modelo_ouro, '') as modelo_ouro,
    nullif(fonte_necessaria, '') as fonte_necessaria,
    nullif(limitacao, '') as limitacao
from {{ ref("cobertura_perguntas_far") }}
