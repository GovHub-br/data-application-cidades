{{ config(materialized="table") }}

-- Ouro: matriz pergunta × dado do painel de perguntas do FAR — o que cada pergunta da
-- oficina com o MCID consegue responder hoje, em qual ouro, o que falta, onde conseguir
-- e qual a limitação.
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
    nullif(limitacao, '') as limitacao,
    nullif(onde_conseguir, '') as onde_conseguir,
    case onde_conseguir
        when 'pedir_mcid' then 'Pedir ao MCid ou aos agentes'
        when 'fonte_publica' then 'Fonte pública'
        when 'pedir_e_publica' then 'Pedir ao MCid e fonte pública'
        when 'outro_orgao' then 'Pedir a outro órgão'
        when 'analise' then 'Falta análise, não fonte'
        when 'sem_fonte' then 'Nenhuma fonte conhecida'
        else 'Já respondida'
    end as onde_conseguir_rotulo
from {{ ref("cobertura_perguntas_far") }}
