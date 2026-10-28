# 0008. Materialização e execução

- **Status:** Proposto
- **Data:** 2026-10-19
- **Relacionados:**
  - ADR-0002 (prata com todos os períodos), ADR-0003 (targets), ADR-0005 (partição
  por carga), ADR-0006 (tipo de carga e portões)
  - Deck *GovHub — Arquitetura Medalhão de Referência* seção 6
  - `dbt/mcid/dbt_project.yml`, `dbt/mcid/profiles.yml`,
  `dbt/mcid/macros/registrar_execucao_dbt.sql`

## Contexto

O deck propõe `dbt build`, tudo como texto na staging e materialização
incremental: bronze por partição de carga, prata por chave de negócio, ouro como
tabela completa. Também avisa que, no laboratório, uma recarga grande de Parquet
foi encerrada por falta de memória, e recomenda medir antes de fixar a estratégia.

Hoje todas as camadas são `table`, por decisão do time de 2026-08-29: a
`materialized_view` derrubava o banco. Só os modelos de `metadata` são
incrementais. Os guardas de memória do DuckDB em `profiles.yml` (`memory_limit`,
`temp_directory`, `preserve_insertion_order: false`) existem por causa das recargas
grandes.

Duas decisões anteriores pesam aqui. Pelo ADR-0002, a bronze e a prata guardam
todas as cargas e todos os períodos, então o volume cresce a cada carga. Pelo
ADR-0006, o tipo de carga de cada fonte (completa ou incremental) fica declarado e
conferido.

Não há medição de tempo por modelo. O `registrar_execucao_dbt` grava em
`lake._dbt_log` só o que falhou.

## Decisão

### 1. `dbt build`

A DAG geral executa `dbt build`. É o que faz os portões do ADR-0006 funcionarem:
um teste `error` que falha na bronze impede a prata de rodar, e um que falha na
prata impede a ouro.

### 2. `table` por padrão

Bronze, prata e ouro continuam `table`. `materialized_view` não volta (decisão de
2026-08-29), e a bronze não vira view (ADR-0001).

### 3. Incremental quando a medição pedir

Um modelo passa a incremental quando a medição mostrar que a reconstrução completa
custa demais (critério abaixo). A estratégia depende da camada e do tipo de carga:

| Camada | Estratégia incremental | Por que é seguro |
|---|---|---|
| Bronze | Insere só as cargas novas (partição `carga`, ADR-0005) | A raw é imutável: uma carga antiga nunca muda |
| Prata, fonte de carga completa | Apaga e reinsere os períodos de referência que as cargas novas tocaram | Cada carga traz a base inteira; a dedup técnica escolhe a carga mais recente do período (ADR-0009) |
| Prata, fonte incremental | Insere ou atualiza pela chave de negócio | Cada carga traz só o que é novo ou mudou |
| Ouro | `table` sempre, a não ser que a medição mostre o contrário | Agrega e é pequena |

### 4. Critério para virar incremental

Um modelo vira incremental quando, por duas semanas seguidas em homologação:

- a reconstrução completa passa de **15 minutos**, ou
- a execução estoura os guardas de memória.

O limite de 15 minutos é uma proposta inicial, a recalibrar depois do primeiro mês
de medição.

### 5. Medir antes de decidir

O `registrar_execucao_dbt` passa a gravar **todos** os modelos em `lake._dbt_log`,
não só as falhas, com tempo de execução e linhas afetadas, que o dbt já expõe em
`results`. É a base para o critério acima e para a decisão do ADR-0003 sobre migrar
os domínios DuckDB.

### 6. Reconstrução completa periódica

Todo modelo incremental passa por um `--full-refresh` semanal, em horário de baixo
uso, e sempre que o contrato da sua fonte mudar. Isso corrige qualquer divergência
que o incremental tenha acumulado.

### 7. Memória

- Os guardas do DuckDB em `profiles.yml` continuam.
- O eixo histórico em `homolog_duckdb` continua serializado (pool de 1 slot,
  ADR-0003).
- Prata e ouro no Postgres continuam com `duckdb.force_execution = true`.

## Alternativas consideradas

**Incremental em tudo desde o começo, como sugere o deck.** Traz a complexidade do
incremental (estado entre execuções, reconstrução periódica, casos de borda) para
modelos que talvez nunca precisem. A falta de memória do laboratório veio de uma
recarga completa de Parquet, não de modelos `table` em geral.

**`materialized_view`.** Derrubou o banco em 2026-08-29.

**View na prata e na ouro.** Toda consulta de painel recalcularia a cadeia inteira.

**Snapshots do dbt (`dbt snapshot`) para o histórico.** Tratado no ADR-0009.

## Trade-offs

| Ganha | Paga |
|---|---|
| Simplicidade enquanto o volume permite | Reconstruções completas ficam mais lentas à medida que as cargas se acumulam |
| Incremental só onde a medição justifica | Precisa de medição contínua e de alguém olhando |
| Estratégia incremental segura, porque a raw é imutável | Duas estratégias de prata, conforme o tipo de carga |
| Divergências do incremental corrigidas toda semana | Uma janela semanal de reconstrução |

## Consequências

- `registrar_execucao_dbt` muda para gravar todos os resultados.
- Modelos incrementais precisam de chave de partição e de período bem definidos, o
  que depende do contrato da fonte (ADR-0006) e do período de referência (ADR-0009).
- A DAG geral ganha uma execução semanal com `--full-refresh` para os modelos
  incrementais.

## Plano de migração

1. Mudar o `registrar_execucao_dbt` e acompanhar um mês em homologação.
2. Listar os modelos acima do critério.
3. Antes do primeiro incremental no Postgres, testar a estratégia de apagar e
   reinserir junto com o `duckdb.force_execution` do pré-hook. O pg_duckdb executa
   leituras; a escrita incremental precisa ser confirmada.
4. Converter os modelos da lista, um por PR, com a comparação de números antes e
   depois (ADR-0002).

## Validação operacional

- Todo modelo aparece em `lake._dbt_log` a cada execução, com tempo e linhas.
- Todo modelo incremental tem execução com `--full-refresh` nos últimos 7 dias.
- Nenhum modelo está materializado como `materialized_view`.

## Questões em aberto

1. **Limite de 15 minutos.** Confirmar ou recalibrar depois do primeiro mês de
   medição.
