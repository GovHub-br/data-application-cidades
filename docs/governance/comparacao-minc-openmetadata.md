# A integração de OpenMetadata que já existia

Levantado em 2026-09-01, comparando este repositório com
`data-application-minc` (branch `main`).

## O achado

A integração do MCID com o OpenMetadata **já existe neste repositório**, na
branch `origin/refactor/openmetadata` (commit `4d6d9ae`, 2026-08-18). Ela nunca
foi mergeada na `main`, está **29 commits atrás** dela e usa o layout antigo
`airflow_lappis/dags/`, que a `main` já não tem — por isso não aparece em
nenhuma busca feita a partir da árvore atual.

O que a branch contém:

| | |
|---|---|
| Recipes de conector | `postgres_metadata`, `postgres_profiler`, `postgres_classifier`, `dbt_metadata`, `airflow_metadata`, `superset_metadata` |
| Orquestração | `openmetadata_ingestion_dag.py` — DAG agendada do Airflow |
| Glossário | `glossaries/mcid.csv` (63 termos) + `mcid.yaml`, com `sync_glossary` idempotente |
| Relações semânticas | `semantic_relationships/mcid.yaml`, 1.255 linhas, publicadas como Custom Properties |
| Governança no dbt | `meta.openmetadata` (domínio, tier, dono, glossário) em **279 pontos** dos `schema.yml` |
| Testes | 3 arquivos |
| Documentação | `README.md`, `DOCUMENTATION_COVERAGE.md`, `RELATIONSHIP_COVERAGE.md` |

**A MinC portou essa integração daqui e a evoluiu.** O
`semantic_relationships.py` deles ainda valida
`kind: MCIDSemanticRelationshipCatalog`, e o guia deles registra: *"é do
Ministério das Cidades, de onde a integração foi portada"*. É também a origem
das propriedades `mcidRelatedTables` e `mcidSemanticRelationships` que existem
na instância sem estarem declaradas em lugar nenhum da `main`.

**E explica a ingestão parada:** o catálogo do `Cidades` não é atualizado desde
21-23/07 porque a DAG que o atualizaria nunca chegou à `main`.

## As duas abordagens, lado a lado

| | branch / MinC (conectores) | este trabalho (REST declarativo) |
|---|---|---|
| Tabela, coluna, descrição | `postgres_metadata` + `dbt_metadata` | script próprio |
| Testes do dbt como test case | automático | — |
| Superset, Airflow | recipes dedicadas | bloqueado sem credencial |
| Domínio, tier, dono | `meta.openmetadata` no `schema.yml` | `dominios.yml` + patch REST |
| Glossário | CSV + `sync_glossary` | YAML + patch REST |
| Profiler e classifier (PII) | recipes prontas | — |
| Execução | **DAG agendada** | manual |
| Produto de dados | ✗ | ✓ 116 / 13 / 11 ativos |
| Classificação `Uso` | ✗ | ✓ 140/140 |
| Certificação | ✗ | ✓ 140/140 |
| MinIO, containers, linhagem do lake | ✗ | ✓ 46 containers, 33 arestas |
| Linhagem coluna a coluna | ✗ | ✓ 130 arestas, 720 colunas |
| Chaves (`tableConstraints`) | ✗ | ✓ |
| Auditoria instância × repo | ✗ | ✓ `make governance-audit-om` |

## Leitura

**Há redundância real.** Estrutura, descrição, coluna e teste são exatamente o
que os conectores fazem — e fazem melhor, porque rodam sozinhos e em dia. A
forma deles de declarar governança (dentro do `schema.yml`, carregada pelo
conector) é mais leve que dois scripts REST e mantém a governança viajando junto
com o projeto dbt.

**Há complemento real.** Produto de dados, permissão de uso, certificação, o
lake no catálogo, linhagem de coluna, chaves e auditoria não existem lá.

**O caminho é somar, não escolher.** A versão da MinC é a descendente mantida da
nossa própria branch e traz uma seção "Levar para outro projeto" com os quatro
ajustes necessários (`config.py`, `lineage.py`, `recipes/*.yaml` e as Variables
do Airflow).

## Armadilhas registradas por eles que valem para nós

- **`markDeletedTables` tem default `true`.** Rodar `postgres_metadata` contra
  um banco incompleto marca como deletado tudo que o catálogo tem e o banco não.
  Um ambiente restaurado pela metade apaga catálogo inteiro sem avisar.
- **O conector dbt não cria tabela** — ele anexa metadado a tabela existente.
  Quem cria é a `postgres_metadata`; a ordem entre as duas é obrigatória.
- **`dbt docs generate` precisa do banco**, o que é o mesmo bloqueio de VPN que
  já trava HU-07 e o resto da HU-12 aqui.
- **A partir do dbt 1.10, `meta` no topo do modelo E em `config.meta` aborta o
  parse.**
- O classifier mantém `storeSampleData: false` e analisa até 50 linhas para
  detectar PII sem persistir amostra — relevante para a HU-13.
