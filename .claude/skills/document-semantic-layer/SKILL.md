---
name: document-semantic-layer
description: Use ao documentar (criar ou revisar) entidades, dimensões, medidas, métricas e glossário de um modelo dbt de qualquer projeto deste repositório (MCid, IPEA, MIR ou outro futuro), para alimentar o dbt Semantic Layer/MetricFlow e o GraphRAG (graphrag-tais). Use ao adicionar um modelo dbt novo em silver/gold, ao preparar uma tabela para consumo pelo GraphRAG, ou ao criar/revisar semantic_models.yml/metrics.yml.
---

# Documentar camada semântica (dbt Semantic Layer + MetricFlow)

Esta skill é **agnóstica de projeto**: nada aqui assume MCid especificamente.
Cada projeto dbt deste repositório (`airflow_lappis/dags/dbt/<projeto>/`)
define seus próprios schemas, database e profile — a skill descobre isso a
cada execução, nunca hardcoda.

## Antes de começar

1. Identifique o projeto dbt alvo e leia seu `dbt_project.yml`
   (`airflow_lappis/dags/dbt/<projeto>/dbt_project.yml`) para descobrir
   `database`, o `profile`, e o `+schema:` físico de cada camada
   (bronze/silver/gold) declarado em `models:`. Não assuma nomes de schema —
   cada projeto declara os seus.
2. Identifique o(s) modelo(s) alvo (`.sql` em `models/**/`). Prefira a camada
   **gold**; use **silver** só quando não existir gold para o subject area.
   Nunca documente semântica (camadas 1–4) sobre **bronze** — dado bruto/ainda
   não tipado não é base válida.
3. Leia o `schema.yml` já existente do modelo (camada 0). Se a `description`
   da tabela, ou de qualquer coluna candidata a entidade/dimensão/medida,
   estiver vazia ou for insuficiente, **pare aqui** e devolva isso como
   pendência para um humano completar a camada 0 antes de prosseguir — a
   camada 0 é pré-requisito, não algo para a skill inferir.
4. Leia o `.sql` do modelo para confirmar o grain real (o que uma linha
   representa) e cruze com o texto da `description`.
5. Verifique se já existe `semantic_models.yml`/`metrics.yml` para esse
   modelo. Se existir, este é um ciclo de **revisão** — trate o conteúdo atual
   como ponto de partida, não escreva por cima sem justificar cada mudança.
6. Leia `checklist.md` (nesta mesma pasta) por inteiro antes de propor
   qualquer artefato. É a fonte de verdade das regras — não recalcule ou
   reinvente critérios aqui.

## Levantar candidatos (nunca inventar semântica de negócio)

1. **Entidade**: procure, na `description` de tabela/coluna e nos
   `data_tests` existentes (`unique`, `not_null`, teste customizado como
   `row_count_match`), a coluna que ancora o grain. Só declare
   `type: primary` quando houver evidência textual ou de teste — nunca por
   suposição de nome de coluna (`id`, `codigo`, etc. não bastam sozinhos).
2. **Dimensões**: colunas categóricas (baixa cardinalidade sugerida pela
   descrição, códigos, UF, status) e temporais (`data_*`, `dt_*`, `periodo`).
   Uma dimensão categórica com valores codificados (`1=X, 2=Y`) herda a
   legenda já documentada na coluna original — não a reescreva do zero.
3. **Medidas**: colunas numéricas com semântica de valor monetário ou
   contagem. Escolha `agg` a partir do que a `description` original já diz
   sobre a coluna. Se a descrição não permitir decidir `agg` com segurança,
   **não escolha por conta própria** — marque
   `agg: PENDENTE_REVISAO_HUMANA` e sinalize no relatório final.
4. **Métricas**: proponha somente a partir de medidas já levantadas no passo
   anterior. Nunca proponha uma métrica sem medida correspondente já
   documentada nesta mesma execução.
5. **Glossário**: toda sigla/abreviação usada nos nomes propostos (entidade,
   dimensão, medida, métrica) que não tenha explicação na `description`
   original vira um item pendente de glossário — não invente o significado
   da sigla.

## Produzir e validar

1. Escreva (ou atualize) `semantic_models.yml` e `metrics.yml` **ao lado do
   `schema.yml`** da camada usada (gold ou silver), seguindo os exemplos de
   `checklist.md`.
2. Rode o checklist completo de `checklist.md`, item por item, contra o que
   você propôs. Todo item que falhar vira uma pendência explícita no
   relatório final — não é aceitável entregar um YAML "quase completo" sem
   listar o que falta.
3. Nunca sobrescreva uma `description` humana já existente na camada 0 para
   "encaixar" no modelo semântico. Se a description existente for
   insuficiente para as camadas 1–4, isso é pendência de camada 0 — não
   licença para reescrevê-la por conta própria.
4. Faça as mudanças numa branch dedicada
   (`docs/semantic-layer-<projeto>-<subject-area>`), nunca direto em `main`.
   Não faça commit/push/PR sem confirmação explícita de quem pediu a
   documentação.

## Relatar

Ao final, relate de forma concisa:

- projeto e modelo(s) documentados, e a camada usada (gold/silver);
- o que foi proposto em cada camada (entidades, dimensões, medidas, métricas,
  glossário);
- toda pendência (`agg: PENDENTE_REVISAO_HUMANA`, glossário faltante,
  description de camada 0 insuficiente, entidade sem evidência) — nunca
  omita;
- itens do checklist que passaram e os que falharam;
- que o resultado é um **rascunho para revisão humana**, não documentação
  aprovada — precisa de validação por quem conhece o domínio de negócio do
  projeto antes de qualquer merge.

Nunca declare "documentação completa" enquanto houver pendência aberta no
relatório.
