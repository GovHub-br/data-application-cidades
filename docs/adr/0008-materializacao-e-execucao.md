# 0008. Materialização e execução

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (fatos conferidos; memória e execução no padrão único do ADR-0003; incremental da bronze pela `_load_id`; limites só no playbook; nomes de rastreio em inglês, ADR-0007)
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
`materialized_view` derrubava o banco. Só dois modelos são
incrementais: `metadata/models_metadata` e
`conjuntura_dbt/qualidade/ouro_conjuntura_qualidade_schema`. Os guardas de memória
dos targets DuckDB em `profiles.yml` (`memory_limit`, `temp_directory`,
`max_temp_directory_size`, `preserve_insertion_order: false`) existem por causa das recargas
grandes.

Duas decisões anteriores pesam aqui. Pelo ADR-0002, a bronze e a prata guardam
todas as cargas e todos os períodos, então o volume cresce a cada carga. Pelo
ADR-0006, o tipo de carga de cada fonte (completa ou incremental) fica declarado e
conferido.

Não há medição de tempo por modelo. O `registrar_execucao_dbt` grava em
`lake._dbt_log` só o que não terminou em sucesso, e só no target do Postgres.

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
| Bronze | Insere só as cargas novas (pela `_load_id`, ADR-0005) | A raw é imutável: uma carga antiga nunca muda |
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
`results`. É a base para o critério acima e para ajustar, por PR no playbook, os
limites do pg_duckdb (ADR-0003).

### 6. Reconstrução completa periódica

Todo modelo incremental passa por um `--full-refresh` semanal, em horário de baixo
uso, e sempre que o contrato da sua fonte mudar. Isso corrige qualquer divergência
que o incremental tenha acumulado.

### 7. Memória

- Com um target só (ADR-0003), os guardas de memória deixam o `profiles.yml` e os
  scripts shell de `dbt/mcid/` e ficam só nos parâmetros do pg_duckdb, no playbook
  do Postgres: `duckdb.max_memory` (4096 MB) e `duckdb.max_temp_directory_size`
  (50 GB).
- O pool `duckdb_historico`, que serializa o eixo histórico, sai junto com o
  `prod_duckdb`.
- Prata e ouro rodam em SQL de Postgres; o `duckdb.force_execution` não é padrão.

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
   reinserir na bronze, que lê o lake pelo pg_duckdb. O pg_duckdb executa leituras;
   a escrita incremental sobre `read_parquet` precisa ser confirmada.
4. Converter os modelos da lista, um por PR, com a comparação de números antes e
   depois (ADR-0002).

## Validação operacional

- Todo modelo aparece em `lake._dbt_log` a cada execução, com tempo e linhas.
- Todo modelo incremental tem execução com `--full-refresh` nos últimos 7 dias.
- Nenhum modelo está materializado como `materialized_view`.

## Questões em aberto

1. **Limite de 15 minutos.** Confirmar ou recalibrar depois do primeiro mês de
   medição.
