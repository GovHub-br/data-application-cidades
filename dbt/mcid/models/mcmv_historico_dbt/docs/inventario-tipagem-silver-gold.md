# Inventário de tipagem — silver/gold histórico + reloginho

Change: `verificar-tipagem-silver-gold-historico`. Levantado 2026-09-07 do
`information_schema` do `cidades.duckdb` (build local, modo A) e do Postgres
prod `10.0.0.50/cidades` (modelos dos colegas).

## 1. Padrão de tipo dos colegas em prod (referência)

Medido em `empreendimento_far.silver_empreendimento` / `.silver_evolucao_financeira`
/ `.gold_ficha_empreendimento`, `entidades_fds.fds_empreendimento`,
`empreendimento_rural.silver_empreendimento`:

| Domínio | Tipo em prod (Postgres) |
|---|---|
| APF, código IBGE, CNPJ, nome, status | `text` |
| Contagem de UH (`quantidade_uh`, `qt_uh_*`), código numérico (`co_*`), `pessoas_atendidas` | `integer` |
| Contagem de linhas/eventos (`qt_liberacoes`) | `bigint` |
| Valor monetário (`valor_*`, `vr_*`) | `numeric` (sem precisão declarada) |
| Percentual (`percentual_*`, `pct_*`, `divergencia_*`) | `numeric` (exceção: `pct_execucao_ts` = `numeric(6,2)`) |
| Data de negócio | `date` |
| Mês truncado (`date_trunc`) | `timestamp with time zone` |
| Coordenada GPS | `numeric` / `numeric(12,8)` / `numeric(6,2)` (inconsistente entre frentes) |
| Indicador (`ic_*`) | `boolean` |
| Ausência de valor | preenchida com `0` / `0.0` (raramente `NULL`) |

Os colegas tipam tudo no **bronze** (`parse_int`→int4, `parse_numeric`→numeric,
`parse_financial_value`→numeric(15,2), `parse_date_br`→date); a silver/gold herda
e alarga por aritmética (`coalesce(x, 0.0)`, `/`, `* 100`) — daí o `numeric`
efetivo ser sem precisão.

## 2. Tipos canônicos deste escopo

O escopo histórico tipa na **silver** (bronze é cópia fiel — convenção medalhão
§6). Tipo canônico de saída por domínio:

| Domínio | Tipo canônico (escopo) | Tipo colegas | Divergência |
|---|---|---|---|
| Texto / código (APF, IBGE, CNPJ, nome, status, fonte, hash) | `varchar` (DuckDB) / `text` (PG) | `text` | **nenhuma** — `varchar` sem limite = `text` no attach |
| Contagem de UH | `bigint` | `integer` | **deliberada** — ver §3.1 |
| Contagem de linhas/eventos (`n_apf`, `n_eventos`, `n_registros`, `n_meses_observados`) | `bigint` | `bigint` | nenhuma |
| Valor monetário na silver | `numeric(15,2)` (`parse_hist_numeric`) | `numeric` | **deliberada** — precisão explícita, ver §3.2 |
| Valor monetário agregado no gold | `numeric(38,2)` (`sum` alarga) | `numeric` | aceitável — `sum` de `numeric(15,2)` |
| Percentual | `double` (`parse_hist_double`, subtração) | `numeric` | **deliberada** — ver §3.3 |
| Data de negócio | `date` | `date` | nenhuma |
| Mês truncado no gold (`mes`, `ouro_dhist_serie_situacao_mensal`) | `date` | `timestamp with time zone` | **deliberada** — `date` é o correto para 1º-do-mês |
| Timestamp técnico (`dt_ingest`, `dt_silver`, `dt_gold`) | `timestamp with time zone` | `timestamp with time zone` | nenhuma |
| Indicador (`marcos_coerentes`) | `boolean` | `boolean` | nenhuma |
| Ano / mês numérico (`ano`, `mes` em `ouro_dhist_serie_mensal`) | `bigint` (`year()`/`month()`) | — | tolerada — `bigint` de função de data |
| `prioridade_familia` | `integer` (literais em `case`) | — | tolerada |
| Ausência de valor | `NULL` | `0` / `0.0` | **deliberada** — ver §3.4 |

## 3. Divergências deliberadas vs. prod

### 3.1 Contagem de UH = `bigint` (prod: `integer`)

`sum()` de UH no `ouro_dhist_serie_mensal` a nível nacional chega a ~1,5 milhão hoje e
a série pré-2018 pode crescer; `bigint` domina `integer` sem custo de
armazenamento relevante no Postgres e elimina risco de overflow em qualquer
agregação futura. Macro: `parse_hist_bigint`. Aplicado uniformemente na silver
e propagado ao gold.

### 3.2 Valor monetário na silver = `numeric(15,2)` (prod: `numeric` sem precisão)

Decisão D10 da change arquivada `vocabulario-e-qualidade-financeira-historica`
(ver memória `valores-financeiros-historico-auditoria-e-proposta`). `numeric(15,2)`
(máx. ~R$ 9,9 tri) cobre com folga qualquer valor de empreendimento e qualquer
soma mensal nacional (investimento total do MCMV histórico ~R$ 300 bi). A
precisão explícita documenta a granularidade (centavos) e barra lixo de origem
com escala maior. Macro: `parse_hist_numeric`. No gold, `sum()` alarga para
`numeric(38,2)` — comportamento correto.

### 3.3 Percentual = `double` (prod: `numeric`)

Percentual de execução não precisa de aritmética decimal exata; `double` é
uniforme em todo o escopo (`parse_hist_double`, `gap_fisico_financeiro_pp` =
`double − double`). O prod dos colegas é misto (`numeric` na maioria,
`numeric(6,2)` no PTS) — inconsistência deles, não padrão a espelhar.

### 3.4 Ausência de valor = `NULL` (prod: `0` / `0.0`)

Série histórica: `0` contratos num mês é informação; ausência de snapshot é
outra coisa. Os golds que somam aplicam `coalesce(sum(x), 0)` no ponto de
agregação, nunca no dado. Segue D10.

## 4. Divergências a corrigir (NÃO deliberadas)

### 4.1 `HUGEINT` (int128) nas colunas de UH agregadas

`SUM(bigint)` no DuckDB produz `HUGEINT`, que **não tem equivalente no
Postgres** — quebra a materialização no modo C (`prod_duckdb`) / publicação.
Colunas afetadas:

| Modelo | Colunas |
|---|---|
| `prata_dhist_serie_executiva` | `uh_contratadas`, `uh_entregues`, `uh_concluidas`, `uh_em_obras`, `uh_comercializadas` |
| `ouro_dhist_serie_mensal` | `uh_contratadas`, `uh_entregues`, `uh_concluidas`, `uh_em_obras`, `uh_comercializadas` |
| `ouro_dhist_serie_situacao_mensal` | `uh`, `entradas`, `saidas` |
| `ouro_reloginho_indicadores` | `uh_contratadas`, `uh_entregues`, `uh_vigentes` |
| `ouro_reloginho_indicadores_frente` | `uh_contratadas`, `uh_entregues`, `uh_vigentes` |
| `ouro_reloginho_indicadores_entregas` | `uh_entregues_evento_mes`, `uh_entregues_evento_acum`, `n_eventos`, `uh_entregues_snapshot`, `dif_evento_vs_snapshot` |
| `ouro_reloginho_resumo_dashboard` | `uh_contratadas_ultimo`, `uh_entregues_ultimo`, `uh_vigentes_ultimo` |
| `prata_dhist_snh_entregas_mes` | `uh_entregues_evento_mes` |

Valores reais são pequenos (máx. nacional ~1,5 M) — cabem em `bigint`. Correção:
`cast(sum(...) as bigint)` (ou `::bigint` no `coalesce`). Feito nesta change —
ver `tasks.md` §4b.

## 5. Cobertura de `verificacao_tipagem`

`verificacao_tipagem` (macro reescrita nesta change para resolver a tabela pelo
relation do modelo, `model.schema` / `model.identifier`, sem schema hard-coded)
declarada nos `schema.yml` das colunas-chave — id/chave, contagem de UH, valor
R$, data, percentual. `tipo_esperado` nos nomes do DuckDB (`BIGINT`, `VARCHAR`,
`DECIMAL(15,2)`, `DECIMAL(38,2)`, `DOUBLE`, `DATE`, `BOOLEAN`).

Cobertura (build local, modo A):

| domínio | tabelas | tests |
|---|---|---|
| `mcmv_historico_dbt` silver | 3 por-frente (via anchor) + entrega_apf + obra_mensal + serie_executiva | ~65 |
| `mcmv_historico_dbt` gold | serie_mensal, serie_situacao_mensal, marco_empreendimento, snapshot_empreendimento_atual | 20 |
| `indicadores_mcmv_dbt` silver | snh_apf_mes, snh_entregas_mes | 12 |
| `indicadores_mcmv_dbt` gold | indicadores_reloginho (+_frente, +_entregas), resumo_reloginho_dashboard | 21 |

**Fora de escopo:** `ouro_reloginho_indicadores_gargalo_desempenho` /
`ouro_reloginho_resumo_gargalo_desempenho_dashboard` (lineage do medalhão FAR/FDS dos
colegas, não da série histórica).

Resultado: **todos os `verificacao_tipagem` PASS** no `dbt test` local
(reloginho + histórico). Numa ida a Postgres os `tipo_esperado` precisam de
revisão (nomes de `data_type` mudam) — anotado no macro e em
`convencao-testes-qualidade.md`.
