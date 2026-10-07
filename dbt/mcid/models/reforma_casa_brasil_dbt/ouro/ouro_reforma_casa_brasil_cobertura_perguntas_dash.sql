{{ config(materialized="table") }}

-- Matriz explícita para o dashboard não transformar ausência de fonte em zero.
-- A Gold só declara como respondida uma pergunta cuja evidência está presente
-- nas bases administrativas protegidas; os demais casos registram a lacuna.
with matriz as (
    select *
    from (
        values
            (
                'caracterizacao',
                'Qual é o perfil dos beneficiários?',
                'disponivel_apos_staging_protegido',
                'Perfil agregado por território, renda, raça/cor, sexo e vínculo com o CadÚnico.',
                'Reforma Casa Brasil + CadÚnico Pessoa e Família pseudonimizados',
                'Depende da materialização do CadÚnico protegido; idade não é recuperável se a data de nascimento não for preservada na camada protegida.'
            ),
            (
                'caracterizacao',
                'Quais tipos de reforma foram realizados?',
                'lacuna_de_fonte',
                'Não há item, serviço, orçamento de obra ou escopo físico da reforma na base administrativa atual.',
                'Projeto aprovado, planilha orçamentária, medição ou vistoria da obra',
                'Modalidade e tipo de imóvel não devem ser usados como substitutos do tipo de reforma.'
            ),
            (
                'acesso',
                'Quem acessa o CadÚnico?',
                'disponivel_apos_staging_protegido',
                'Quantidade e proporção de contratos vinculados por identificador pseudonimizado.',
                'Join HMAC entre Reforma Casa Brasil e CadÚnico',
                'A Gold expõe somente agregados e aplica supressão de células pequenas.'
            ),
            (
                'acesso',
                'Conhece e entende o programa?',
                'lacuna_de_fonte',
                'Não há resposta de conhecimento, compreensão ou canal de informação na base atual.',
                'Questionário de acesso ou pesquisa com o público',
                'Não inferir conhecimento a partir da contratação.'
            ),
            (
                'acesso',
                'Conseguiu acessar, desistiu ou foi recusado? Por quê?',
                'lacuna_de_funil',
                'A carteira contratada mostra somente quem chegou ao contrato.',
                'Funil de propostas, recusas, desistências, motivo e canal de atendimento',
                'Sem denominador de pessoas/propostas não é possível medir acesso ou abandono.'
            ),
            (
                'acesso',
                'Quais são os desafios de acesso ao programa?',
                'parcial_cobertura_de_vinculo',
                'A cobertura do vínculo com o CadÚnico pode indicar alcance do cruzamento, mas não mede barreiras de acesso.',
                'Funil de propostas, motivos de desistência/recusa e pesquisa de usuários',
                'Sem proposta, reprovação, desistência e motivo, não é possível estimar gargalos de acesso.'
            ),
            (
                'implementacao',
                'Valor, prazo, juros, prestação e situação administrativa do contrato',
                'disponivel',
                'Indicadores agregados de financiamento, recursos próprios, FGTS, prazo, prestação e atraso.',
                'GEFUS/FGTS',
                'São atributos administrativos do contrato, não avaliação de experiência da família.'
            ),
            (
                'implementacao',
                'O financiamento foi adequado à necessidade da reforma?',
                'parcial_atributos_contratuais',
                'Valor, prestação, prazo e renda declarada permitem caracterizar o contrato.',
                'Necessidade da família, projeto e escopo físico da reforma',
                'Sem tipo e custo da reforma não é possível avaliar suficiência ou adequação do financiamento.'
            ),
            (
                'implementacao',
                'Burocracia, facilidade de contratação, documentação, relação com a instituição financeira e compreensão das condições',
                'lacuna_de_fonte',
                'Não há percepção da família nem registro padronizado do atendimento na base atual.',
                'Questionário, atendimento do agente financeiro ou pesquisa de satisfação',
                'Não inferir percepção a partir de prazo, juros ou atraso.'
            ),
            (
                'implementacao',
                'Obra bem feita e no prazo',
                'lacuna_de_fonte',
                'Não há medição, vistoria, cronograma físico ou aceite da obra na fonte atual.',
                'Medição, vistoria, assistência técnica ou pesquisa pós-obra',
                'A situação do contrato não comprova qualidade ou prazo da reforma.'
            ),
            (
                'resultado',
                'O que foi reformado e a obra foi concluída?',
                'lacuna_de_fonte',
                'A fonte atual não contém item de obra, escopo, medição, vistoria ou aceite.',
                'Projeto, planilha orçamentária, medição, vistoria e aceite da obra',
                'Contrato, modalidade e tipo de imóvel não comprovam item reformado nem conclusão.'
            ),
            (
                'resultado',
                'Quanto foi gasto na reforma?',
                'parcial_valores_contratuais',
                'A carteira contém financiamento, investimento, desconto, recursos próprios e FGTS utilizados.',
                'Pagamento efetivo, desembolso, nota/fatura e medição de obra',
                'Os valores contratuais não comprovam gasto efetivo na obra.'
            ),
            (
                'resultado',
                'Houve redução da inadequação habitacional?',
                'proxy_linha_de_base',
                'Condições domiciliares do CadÚnico permitem medir a inadequação antes da reforma em agregados.',
                'CadÚnico Família antes e após a obra, ou vistoria comparável',
                'Sem observação pós-obra, a Gold não mede redução nem atribui causalidade ao programa.'
            ),
            (
                'resultado',
                'Houve melhoria na vida das famílias?',
                'lacuna_pos_obra',
                'Não há medida pós-obra de conforto, segurança, salubridade ou bem-estar.',
                'Pesquisa ou vistoria pós-obra com linha de base comparável',
                'Não usar a existência do contrato como evidência de melhoria.'
            ),
            (
                'resultado',
                'Quais foram os impactos financeiros para as famílias?',
                'parcial_linha_de_base',
                'Renda e despesas declaradas do CadÚnico podem ser combinadas a prestação e recursos do contrato para caracterizar a situação inicial.',
                'CadÚnico Família protegido + GEFUS/FGTS + acompanhamento posterior',
                'Sem renda, despesas ou inadimplência observadas após a reforma, não há medida de impacto financeiro realizado.'
            ),
            (
                'monitoramento',
                'Como está a execução de recursos?',
                'parcial_posicao_carteira',
                'Séries de snapshots permitem monitorar carteira contratada, valor financiado, recursos próprios, FGTS utilizado e atraso.',
                'Histórico GEFUS/FGTS por data de referência',
                'Não substitui execução orçamentária, pagamento, desembolso nem medição física de obra.'
            ),
            (
                'monitoramento',
                'Qual é a execução orçamentária e física da reforma?',
                'lacuna_de_fonte',
                'A fonte atual não contém liquidação, pagamento, cronograma físico ou medição de obra.',
                'Execução orçamentária, agente financeiro e medição/vistoria de obra',
                'A Gold deve apresentar esta lacuna de forma explícita no dashboard.'
            ),
            (
                'avaliacao',
                'Quais características aumentam ou reduzem a probabilidade de acesso e de melhoria habitacional?',
                'parcial_previsao_de_carteira',
                'A série permite previsão de contratação e valores da carteira.',
                'Funil de acesso, tipo de reforma e resultado pós-obra em série longitudinal',
                'Sem desfecho de acesso e melhoria não é possível estimar efeito ou impacto habitacional.'
            )
    ) as t(
        bloco,
        pergunta,
        situacao_dado,
        resposta_disponivel,
        fonte_necessaria,
        limitacao
    )
)

select *
from matriz
