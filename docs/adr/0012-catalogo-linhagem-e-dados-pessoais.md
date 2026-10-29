# 0012. Catálogo, linhagem e dados pessoais

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (nome das macros; eixo histórico fora do catálogo desde o PR #241; catálogo de colunas)
- **Relacionados:**
  - ADR-0002 (R0, pseudonimização), ADR-0006 (contrato da fonte), ADR-0007 (sufixo
  `_token`), ADR-0010 (schema por secretaria)
  - `infra/playbooks/openmetadata/`, `dags/openmetadata_ingestion_dag.py`,
  `dbt/mcid/models/docs/procedimento-artefatos-openmetadata.md`,
  `dbt/mcid/macros/coluna_sensivel.sql`, `dbt/mcid/tests/conjuntura_sem_dado_sensivel.sql`

## Contexto

O catálogo já existe:

- O OpenMetadata 1.13.3 sobe por playbook, com o banco na VM do Postgres.
- Depois de cada execução do dbt, a DAG geral dispara a
  `openmetadata_ingestion_dag`, que ingere `manifest.json`, `catalog.json` e
  `run_results.json`. Isso leva ao catálogo as descrições, as tags, o `meta`, a
  linhagem entre modelos e o resultado dos testes. Desde o PR #241, o
  `docs generate` roda com o seletor `dominios_postgres`, porque o eixo histórico não
  compila no target do Postgres. Por isso `mcmv_historico_dbt`,
  `reforma_casa_brasil_dbt` e `linha_financiada_dbt` ficam sem catálogo (sem os tipos
  das colunas) no OpenMetadata até a migração do ADR-0003.
- O `dbt_project.yml` declara por pasta um `+meta.governance` (`product`,
  `owner_key`, `classification`, `rag_publication`) e um `+meta.openmetadata`
  (`domain`, `tier`, `owner`), herdados pelos modelos.
- As macros de `macros/coluna_sensivel.sql` (`padroes_sensiveis`, `parece_cpf`,
  `parece_dado_pessoal`) detectam colunas de dado pessoal pelo nome (`cpf`, `nis`,
  `endereco`...) e pelo conteúdo (CPF com dígito verificador válido). O teste
  `conjuntura_sem_dado_sensivel` falha o build se uma coluna assim aparecer numa
  camada persistida. Hoje só cobre o domínio `conjuntura`.

Falta dizer quem é a fonte da verdade dos metadados, o que é obrigatório em cada
modelo e como a pseudonimização do ADR-0002 aparece no catálogo.

## Decisão

### 1. O dbt é a fonte da verdade; o OpenMetadata é a vitrine

Descrições de tabela e coluna, tags, classificação e dono ficam nos arquivos do dbt
e são revisados em PR. O OpenMetadata recebe tudo isso a cada execução. Editar no
OpenMetadata algo que o dbt carrega não adianta: a próxima ingestão sobrescreve.

No OpenMetadata se edita só o que o dbt não carrega: o glossário e as conversas.

### 2. Metadados obrigatórios

Todo modelo herda da pasta do seu domínio, no `dbt_project.yml`:

| Chave | Valores | Para quê |
|---|---|---|
| `governance.product` | Domínio (`far`, `conjuntura`...) | Agrupa o produto de dados |
| `governance.owner_key` | Área dona | Quem responde pelo dado |
| `governance.classification` | `public`, `internal`, `restricted` | Quem pode ver |
| `governance.rag_publication` | `prohibited`, `eligible_after_security_validation` | Se a tabela pode alimentar RAG ou LLM |
| `openmetadata.domain`, `tier`, `owner` | Do catálogo | Organização no OpenMetadata |

O padrão de `rag_publication` é `prohibited`. Uma tabela só passa a
`eligible_after_security_validation` depois de revisão de segurança.

Toda fonte em `sources.yml` também tem descrição e contrato (ADR-0006), e toda
coluna de prata e ouro tem descrição.

### 3. Linhagem

A linhagem vem do `manifest.json`: fonte, bronze, prata e ouro. Como cada fonte
declara o `meta.caminho` da staging (ADR-0005), o catálogo mostra de que caminho do
lake a bronze lê. A ligação entre a raw e a staging fica no `lake._staging_log`, que
registra cada objeto convertido.

### 4. Dados pessoais

- Colunas pseudonimizadas (ADR-0002 R0) levam o sufixo `_token` (ADR-0007) e a tag
  `PII.Pseudonimizado` no catálogo. A descrição diz de que identificador o token
  vem e lembra que, para quem tem a chave, ele continua sendo dado pessoal.
- O teste de dado sensível passa a cobrir **todos** os domínios, não só a
  conjuntura. É a segunda linha de defesa: se o mascaramento falhar antes, o build
  falha aqui.
- A regra por nome passa a aceitar o sufixo `_token`. A detecção por conteúdo (CPF
  com dígito verificador válido) continua valendo para toda coluna, inclusive as
  `_token`.

### 5. Glossário

O glossário de negócio (`glossario-mcid.md`, `glossario-valores-financeiros.md`)
continua no repositório como texto de referência e é publicado como glossário no
OpenMetadata. Os termos são ligados às colunas das fontes pelo `term` do contrato
(§6) e às colunas das pratas e ouros pela tag do termo, declarada no dbt.

### 6. Catálogo de colunas

O catálogo de colunas do ADR-0006 (`term` e `aliases` de cada coluna do contrato)
tem duas formas de consulta:

- **No OpenMetadata,** o `term` é publicado como termo do glossário ligado à
  coluna. A página do termo mostra todas as colunas, de todas as fontes e camadas,
  que significam aquilo, e a linhagem liga a coluna da fonte às dos modelos. É a
  forma navegável do grafo, sem serviço novo.
- **No banco,** o modelo `metadata.column_catalog` é montado do grafo do dbt a
  cada execução, com uma linha por fonte, coluna, `term` e alias. Ele pode ser
  cruzado com o `lake._staging_log`, por exemplo para listar as renomeações
  conhecidas (`known_rename`) de cada fonte ao longo do tempo.

## Alternativas consideradas

**OpenMetadata como fonte da verdade, editado pela interface.** É mais acessível
para quem não usa git, mas as mudanças não passam por revisão, e a próxima ingestão
do dbt apagaria parte delas.

**Só o site do `dbt docs`.** Já sai do dbt sem esforço, mas não tem glossário,
busca entre projetos, dono nem tags de dado pessoal.

**Teste de dado sensível só por nome.** Uma coluna `observacao` com CPF dentro, ou
uma coluna renomeada, passaria. Por isso a detecção por conteúdo continua.

## Trade-offs

| Ganha | Paga |
|---|---|
| Metadado revisado em PR junto com o código | Quem não usa git depende do time para mudar uma descrição |
| Catálogo sempre igual ao que roda | Descrição faltando aparece como falha no CI |
| Segunda linha de defesa contra dado pessoal em todos os domínios | O teste por conteúdo custa tempo nas tabelas grandes |
| Pseudonimização visível para quem consulta o catálogo | Tag e descrição a manter em cada coluna `_token` |

## Consequências

- O teste `conjuntura_sem_dado_sensivel` vira um teste para todos os domínios, com
  a exceção do sufixo `_token` na regra por nome.
- Nasce o modelo `metadata.column_catalog`.
- A troca de schemas do ADR-0007 muda o endereço das tabelas no catálogo
  (`cidades.snh.<tabela>`). A ingestão seguinte recria as entradas, e as antigas
  saem.

## Plano de migração

1. Generalizar o teste de dado sensível para todos os domínios.
2. Acrescentar o teste sobre o grafo dos metadados obrigatórios e completar o que
   faltar, domínio a domínio.
3. Criar a tag `PII.Pseudonimizado` no OpenMetadata e marcar as colunas `_token`
   conforme forem surgindo.
4. Publicar o glossário no OpenMetadata.
5. Publicar o `term` das colunas do contrato como termo do glossário e criar o
   `metadata.column_catalog`.

## Validação operacional

- Teste sobre o grafo, no CI: todo modelo tem `governance.product`, `owner_key`,
  `classification` e `rag_publication`; toda coluna de prata e ouro tem descrição.
- O teste de dado sensível roda em todos os domínios e passa.
- Todos os domínios aparecem no catálogo com os tipos das colunas.
- Toda coluna `_token` tem a tag `PII.Pseudonimizado` no catálogo.
- Todo `term` declarado no contrato existe no glossário publicado.

## Questões em aberto

Nenhuma.
