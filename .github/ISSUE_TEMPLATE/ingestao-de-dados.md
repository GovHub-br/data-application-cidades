---
name: Ingestão de Dados
about: Criar ou ajustar processo de ingestão de dados
title: ''
labels: ''
assignees: ''

---

## Contexto

Descreva a necessidade de ingestão dos dados.

## Objetivo da ingestão

Explique o que precisa ser ingerido e para qual finalidade.

## Fonte dos dados

- Nome da fonte:
- Tipo da fonte: API / banco / planilha / CSV / parquet / scraping / outro
- Localização:
- Responsável:
- Frequência de atualização:
- Status do acesso:

## Destino dos dados

- Banco/schema:
- Tabela:
- Ambiente:
- Camada: raw / bronze / silver / gold

## Estratégia de ingestão

- [ ] Carga completa
- [ ] Carga incremental
- [ ] Carga particionada
- [ ] Carga sob demanda
- [ ] Outra

## Chaves e controle

- Chave primária:
- Campo de data de atualização:
- Regra para duplicidades:
- Regra para reprocessamento:
- Existe histórico a ser mantido?

## Tarefas

- [ ] Validar fonte de dados
- [ ] Validar acesso
- [ ] Definir estratégia de ingestão
- [ ] Criar estrutura de destino
- [ ] Implementar ingestão
- [ ] Tratar duplicidades
- [ ] Criar controle de execução
- [ ] Validar dados ingeridos
- [ ] Documentar processo

## Critérios de aceite

- [ ] Dados são ingeridos no destino esperado
- [ ] Estratégia de ingestão está definida
- [ ] Processo pode ser reexecutado com segurança
- [ ] Duplicidades foram tratadas ou justificadas
- [ ] Dados ingeridos foram validados
- [ ] Documentação mínima foi criada

## Observações

Adicione links, prints, exemplos de dados, erros ou referências.