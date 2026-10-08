{{ config(materialized="table") }}

-- Matriz explícita para o dashboard não apresentar ausência de dado como resultado zero.
select *
from (
    values
        ('acesso', 'Perfil de quem acessa', 'disponivel', 'Reforma + CadÚnico'),
        ('acesso', 'Raça/cor de quem acessa', 'disponivel_agregado', 'CadÚnico Pessoa'),
        ('acesso', 'Quem acessa o CadÚnico', 'disponivel', 'Join HMAC CPF/NIS'),
        ('acesso', 'Conhece ou entende o programa', 'requer_pesquisa', 'Questionário'),
        ('acesso', 'Desistiu e por quê', 'requer_pesquisa', 'Questionário/funil de propostas'),
        ('implementacao', 'Valor, prazo, juros e prestação', 'disponivel', 'GEFUS/FGTS'),
        ('implementacao', 'Burocracia e facilidade de contratação', 'requer_pesquisa', 'Questionário'),
        ('implementacao', 'Compreensão das condições', 'requer_pesquisa', 'Questionário'),
        ('implementacao', 'Obra bem feita e no prazo', 'requer_pesquisa_ou_medicao', 'Pesquisa/assistência técnica'),
        ('resultado', 'Inadequação anterior à reforma', 'proxy_disponivel', 'CadÚnico Família'),
        ('resultado', 'O que foi reformado', 'requer_pesquisa_ou_execucao', 'Pesquisa/execução da obra'),
        ('resultado', 'Obra concluída', 'requer_pesquisa_ou_execucao', 'Pesquisa/execução da obra'),
        ('resultado', 'Conforto, segurança e salubridade após obra', 'requer_pesquisa', 'Pesquisa pós-obra'),
        ('resultado', 'Impacto por tipo de melhoria', 'requer_pesquisa_longitudinal', 'Linha de base + pós-obra')
) as t(bloco, pergunta, situacao_dado, fonte_necessaria)
