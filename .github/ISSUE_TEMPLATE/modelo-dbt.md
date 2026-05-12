---
name: Modelo dbt
about: Criar, alterar ou validar modelos dbt
title: ''
labels: ''
assignees: ''

---

## Contexto

Descreva a necessidade do modelo dbt.

## Objetivo

Explique qual transformação ou modelagem precisa ser feita.

## Modelos envolvidos

- Modelo de origem:
- Modelo intermediário:
- Modelo final:
- Camada: staging / intermediate / mart / gold

## Regras de transformação

Liste as regras esperadas.

- 
- 
- 

## Campos esperados

| Campo | Tipo | Origem | Regra | Obrigatório |
|------|------|--------|-------|-------------|
|      |      |        |       |             |

## Testes esperados

- [ ] Teste de unicidade
- [ ] Teste de não nulo
- [ ] Teste de relacionamento
- [ ] Teste de valores aceitos
- [ ] Teste de regra de negócio
- [ ] Teste de volume/contagem

## Tarefas

- [ ] Mapear fontes
- [ ] Criar ou alterar modelo dbt
- [ ] Criar testes
- [ ] Atualizar documentação
- [ ] Executar `dbt run`
- [ ] Executar `dbt test`
- [ ] Validar resultado com solicitante

## Critérios de aceite

- [ ] Modelo executa sem erro
- [ ] Testes passam com sucesso
- [ ] Campos estão documentados
- [ ] Regras de negócio foram aplicadas
- [ ] Dados foram validados
