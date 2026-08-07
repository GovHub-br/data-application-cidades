# Contexto: integração GraphRAG (graphrag-tais) × dbt Semantic Layer/MetricFlow

> Resumo de uma conversa no repositório `graphrag-tais` que originou a skill
> `document-semantic-layer` nesta pasta. Cole este arquivo (ou peça para ler)
> no início de uma nova sessão neste repositório para retomar o contexto sem
> precisar re-investigar tudo de novo.

## O que é o graphrag-tais

GraphRAG de metadados para *text-to-SQL*: pergunta em PT-BR → recupera
subgrafo de schema (Qdrant + Neo4j) → gera SQL. Metadados vêm do
**OpenMetadata**. É um RAG genérico do GovHub (entrega imediata: MinC;
desenvolvimento/validação: MCid — ADR-0000). Todas as decisões de arquitetura
vivem em `docs/adr/` naquele repositório (ADR-0000 a ADR-0014 no momento
desta conversa).

## Por que esta skill existe

A equipe vai documentar as tabelas usando **dbt Semantic Layer + MetricFlow**,
com dois objetivos: (a) gerar descrições/glossário e (b) resolver
métricas/*grain* para o GraphRAG. Isso ataca um gap citado repetidamente nos
ADRs do graphrag-tais: hoje o sistema não garante correção de *grain*/*fan-out*
(`COUNT` pode contar errado ao fazer JOIN entre tabelas de granularidades
diferentes), e a IR do pipeline de query (ADR-0006) tem campos `concept`/
`metric` que hoje não resolvem contra nada real — são texto que o LLM
preenche sem validação.

## O que já foi decidido/descoberto (no graphrag-tais)

- **ADR-0004**: OpenMetadata é a fonte única de metadados de todo o GovHub —
  qualquer integração nova deveria, em princípio, preservar esse ponto único.
- **ADR-0006**: a IR da pergunta fala em conceitos de negócio (`concept`,
  `metric`), não em nomes físicos — hoje sem lastro real.
- **ADR-0011**: contrato de ingestão (`TableRecord`/`ColumnRecord`/
  `RelationshipRecord`) exclui explicitamente MetricFlow, métricas, *grain* e
  glossário do escopo v1. Também documenta que as relações semânticas do MCid
  hoje chegam ao OpenMetadata via uma Custom Property (`mcidSemanticRelationships`)
  em formato Markdown, publicada por um processo cuja proveniência o ADR-0011
  cita como `airflow_lappis/dags/openmetadata/semantic_relationships/` **neste
  repositório** — **mas uma busca exaustiva (todo o histórico git, todas as
  branches locais e remotas, todas as extensões de arquivo) não encontrou
  nada com esse nome ou conteúdo aqui**. Isso ficou como pendência aberta:
  **descobrir onde esse publicador realmente vive** antes de estender a
  integração (ver "Pendências" abaixo).
- **ADR-0012**: framework de avaliação orientada a métricas (MDD) para o
  retrieval — também exclui MetricFlow do escopo inicial.
- **ADR-0014** (criado nesta conversa, PR #5 em
  `GovHub-br/graphrag-tais`, branch `docs/adr-0014-modelo-documentacao-dbt-semantic-layer`
  contra `feat/pipeline-v2`): define o **modelo de documentação em 5 camadas**
  que esta skill implementa — ver `checklist.md` nesta pasta para o conteúdo
  completo (é a mesma fonte de verdade, sincronizada com o ADR).

## Duas rotas de integração em teste (em paralelo — nenhuma decidida ainda)

- **Rota A**: dbt Semantic Layer alimenta o OpenMetadata (via `semantic_manifest.json`
  publicado como Custom Property estruturada, substituindo o Markdown frágil
  atual) — preserva o princípio de fonte única do ADR-0004.
- **Rota B**: o graphrag-tais consome o manifesto/MetricFlow Server
  diretamente, inclusive em tempo de query, para resolver `metric`/`concept`
  da IR e delegar cálculo grain-safe ao MetricFlow em vez de o LLM escrever
  JOIN cru.

## O que já foi descoberto especificamente **neste repositório**

- Este repositório hospeda **três** projetos dbt, não só MCid:
  `airflow_lappis/dags/dbt/{mcid,ipea,mir}/`. Qualquer skill/documentação
  precisa ser agnóstica de projeto — por isso o `SKILL.md` desta pasta lê o
  `dbt_project.yml` de cada projeto em vez de assumir nomes fixos.
- O dbt do MCid já tem documentação madura na **camada 0**: `schema.yml` com
  `description` em tabela e coluna, `tags`, testes customizados
  (`row_count_match`). As camadas medallion (`bronze`/`silver`/`gold`) batem
  exatamente com os schemas que o graphrag-tais ingere hoje
  (`conjuntura_bronze/silver/gold`, `empreendimento_far`).
- **Não existe** nenhum artefato de Semantic Layer/MetricFlow ainda —
  greenfield total (sem `dbt-metricflow` no `pyproject.toml`, sem chaves
  `semantic_models`/`metrics` em nenhum YAML).
- A coluna `apf` (em `cadastro_pj`/`consolidado`, projeto MCid) já é descrita
  como "chave de integração principal" — forte candidata a `entity primary`
  de referência, usada como exemplo no `checklist.md`.
- Orquestração via Airflow + **Astronomer Cosmos** (`DbtTaskGroup`), já
  rodando o dbt do MCid (`conjuntura_boletim_dag.py`).

## O que já foi feito nesta pasta (`.claude/skills/document-semantic-layer/`)

- `SKILL.md`: orquestra o processo de documentar entidades/dimensões/medidas/
  métricas/glossário de qualquer modelo dbt deste repositório. Regras centrais:
  nunca inventa semântica de negócio sem evidência (description existente ou
  `data_tests`); marca `agg: PENDENTE_REVISAO_HUMANA` quando não há segurança
  para decidir; nunca sobrescreve descrição humana; sempre produz rascunho em
  branch dedicada, nunca commit/push automático.
- `checklist.md`: fonte de verdade das regras/convenção de nomenclatura,
  sincronizada com o ADR-0014 do graphrag-tais.
- **Nada foi commitado ainda** neste repositório — os três arquivos
  (`SKILL.md`, `checklist.md`, este `CONTEXT.md`) estão *untracked*.

## Achado (2026-07-27): o que o graphrag-tais realmente consome hoje

Verificado lendo `src/rag/components/sources/openmetadata_source.py` e
`metadata_record_builder.py` no repo `graphrag-tais` (local, em
`~/Workspace/lablivre/graphrag-tais`):

- A ingestão lê **apenas** `Table`/`Column` da API do OpenMetadata
  (`description`, `tags`, `tableConstraints` para PK/FK, e a Custom Property
  Markdown `mcidSemanticRelationships`). **Nada de `semantic_models.yml`/
  `metrics.yml`/MetricFlow é lido hoje** — bate com a exclusão de escopo do
  ADR-0011. Ou seja: os artefatos que esta skill produz **ainda não têm
  consumidor real** — são preparação para a Rota A/B, não algo já ligado ao
  pipeline.
- O escopo de ingestão vem de `OM_SCHEMAS` (lista de schemas físicos), sem
  noção de bronze/silver/gold no código.
- **Achado crítico**: no `.env.example`, `OM_SCHEMAS` traz 3 schemas
  separados para `conjuntura` (`conjuntura_bronze`, `conjuntura_silver`,
  `conjuntura_gold` — bate com `mcid/dbt_project.yml`, que declara `+schema`
  por camada só para `conjuntura_dbt`). Mas para `empreendimento_far` é **um
  schema físico só** (`Cidades.cidades.empreendimento_far`) — porque esse
  subject area não separa `+schema` por camada no `dbt_project.yml`. Mesmo
  padrão em **todos os subject areas de `ipea` e `mir`** (`contratos`,
  `pessoas`, `ted`, `orcamento`, `emendas`, `dados_abertos`,
  `empenhos_ted_dbt` — nenhum declara `+schema` por camada nos respectivos
  `dbt_project.yml`). **`conjuntura_dbt` é o único subject area de todo o
  repositório com separação física real por camada.**
- Consequência prática: hoje, bronze+silver+gold de `empreendimento_far` (e
  de qualquer subject area de `ipea`/`mir`) já entram misturados, sem
  distinção de camada, em qualquer ingestão que aponte pro schema deles — a
  única exceção é `conjuntura`.
- Decisão tomada com o usuário: para `empreendimento_far_dbt`, documentar
  **gold e silver** (não só gold) nesta rodada, já que a duplicação de
  entidade/medida entre as duas camadas não é um conflito real hoje (nenhum
  consumidor lê os dois ao mesmo tempo ainda) — mas fica registrado que essa
  duplicação **vai precisar de resolução** quando a Rota A/B for
  implementada. Para `conjuntura_dbt` (única camada fisicamente separada),
  manteve-se a regra padrão do `checklist.md`: só gold.

## Pendências / próximos passos

1. **Descobrir onde vive o publicador de `mcidSemanticRelationships`** citado
   pelo ADR-0011 do graphrag-tais — não está neste repositório. Sem isso, a
   Rota A (dbt SL → OpenMetadata) não pode ser desenhada em detalhe além do
   que já está no ADR-0014.
2. **Testar a skill contra um modelo real** — candidato natural:
   `empreendimento_far_dbt` (já tem `apf` bem documentado como chave).
3. **Decidir se/quando commitar** a skill (branch dedicada, ex.:
   `docs/skill-semantic-layer-documentation`).
4. Quando uma rota (A/B) convergir, ou as duas forem aceitas em paralelo,
   volta pro graphrag-tais para abrir o ADR que estende formalmente o
   contrato do ADR-0011 (`MetricRecord`, campos novos em `ColumnRecord`) e a
   resolução de `concept`/`metric` da IR (ADR-0006).
