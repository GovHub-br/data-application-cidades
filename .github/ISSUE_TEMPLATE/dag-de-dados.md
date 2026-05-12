---
name: DAG de Dados
about: Criar, ajustar ou corrigir uma DAG de dados
title: ''
labels: ''
assignees: ''

---

## Contexto

Descreva o motivo da criação ou alteração da DAG.

## Objetivo da DAG

Explique o que a DAG deve executar.

## Fonte dos dados

- Nome da fonte:
- Tipo da fonte:
- Localização:
- Responsável:
- Frequência de atualização:

## Destino dos dados

- Banco/schema:
- Tabela/modelo:
- Ambiente:
- Camada: bronze / silver / gold

## Periodicidade

- Frequência de execução:
- Horário esperado:
- Existe dependência de outra DAG?

## Tarefas

- [ ] Validar fonte de dados
- [ ] Validar acesso
- [ ] Definir estratégia de execução
- [ ] Criar ou alterar DAG
- [ ] Configurar dependências
- [ ] Configurar logs e alertas
- [ ] Testar execução local
- [ ] Testar execução em ambiente de homologação
- [ ] Validar dados gerados
- [ ] Documentar DAG

## Critérios de aceite

- [ ] DAG executa sem erro
- [ ] Dados são gerados no destino esperado
- [ ] Periodicidade está configurada corretamente
- [ ] Dependências foram validadas
- [ ] Logs permitem rastrear falhas
- [ ] Documentação mínima foi criada

## Observações

Adicione links, prints, erros, logs ou referências.
