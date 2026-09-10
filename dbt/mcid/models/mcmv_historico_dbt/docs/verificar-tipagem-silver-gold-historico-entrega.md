# Entrega — verificar-tipagem-silver-gold-historico

Change OpenSpec `verificar-tipagem-silver-gold-historico`. Referência de tipos:
`inventario-tipagem-silver-gold.md` (mesmo diretório).

## Item 1 — variante de coluna `uhs_*` na silver do reloginho

### Problema

`prata_dhist_snh_apf_mes` lia só `uh_contratadas` / `uh_entregues` /
`uh_vigentes` com `try_cast` na mão. A SNH BB reporta essas contagens em
`uhs_contratadas` / `uhs_entregues` / `uhs_vigentes` nos snapshots
**2024-06, 2024-07, 2024-10, 2024-11, 2025-01** (`union_by_name` sobre parquets
de layout que mudou por safra; BB passa a `uh_*` em 2025-03; CAIXA sempre `uh_*`,
sem `uhs_*`).

### Correção

CTE `tipado`: as 3 contagens passam a
`coalesce_present_parsed(ref(f.modelo), ['uh_contratadas','uhs_contratadas'], 'parse_hist_bigint', 'bigint')`
— mesma macro/assinatura do `prata_dhist_snh_arm` das silvers por frente.
`coalesce_present` (introspecção por família em tempo de compilação) é necessário
porque a bronze CAIXA não tem `uhs_*`.

### Diff antes/depois (build local `cidades.duckdb`, modo A)

| Métrica | Antes | Depois |
|---|--:|--:|
| `prata_dhist_snh_apf_mes` linhas totais | 307.731 | 307.731 |
| `uh_contratadas` não-nulo | 301.291 | 307.731 |
| BB: linhas | 20.608 | 20.608 |
| BB: `uh_contratadas` não-nulo | 12.880 | 20.608 |

`ouro_reloginho_indicadores`, agente BB (contratadas / entregues / vigentes):

| mês | antes | depois |
|---|---|---|
| 2024-06 | 0 / 0 / 0 | 167.762 / 145.988 / 21.774 |
| 2024-07 | 0 / 0 / 0 | 167.762 / 145.988 / 21.774 |
| 2024-10 | 0 / 0 / 0 | 167.762 / 145.988 / 21.761 |
| 2024-11 | 0 / 0 / 0 | 167.762 / 145.988 / 21.761 |
| 2025-01 | 0 / 0 / 0 | 167.762 / 145.998 / 21.755 |
| 2025-03+ | inalterado | inalterado |

`assert_reloginho_reconcilia_66` (CAIXA 2026-03): `1.697.630 / 1.391.909` —
**inalterado**, casa com a referência #66.

`assert_reloginho_frente_cobertura_mensal`: segue com 5 linhas (lacuna de
FONTE — BB sem snapshot em 2024-08/09/12, 2025-02/04/11; CAIXA sem 2024-08).
**Não alterado por esta change** — o teste verifica existência de linha por mês,
não valor; as linhas BB desses 5 meses sempre existiram, só tinham UH nula.

### Guarda

`completude_minima` (`warn`, `min_pct: 0.97`) em `uh_contratadas` /
`uh_entregues` / `uh_vigentes` no `prata/schema.yml` — regressão de mapeamento
de coluna (safra futura renomeia de novo) derruba a cobertura e o teste avisa.

### `prata_dhist_snh_entregas_mes` — verificado, sem o problema

A bronze `bronze_snh_entregas` já harmoniza `qt_uh_entregues` (CAIXA) /
`numero_de_unidades_entregues` (BB) via `coalesce_present_cols`, expondo
`qt_uh_entregues_evento` que o modelo só soma.

## Item 4b — `HUGEINT` nas colunas de UH agregadas

`SUM(bigint)` no DuckDB → `HUGEINT` (int128), sem tipo equivalente no Postgres
(quebraria o modo C). Corrigido com `cast(... as bigint)` nas somas de:

| modelo | colunas |
|---|---|
| `prata_dhist_serie_executiva` | 5 `uh_*` |
| `ouro_dhist_serie_mensal` | 5 `uh_*` |
| `ouro_dhist_serie_situacao_mensal` | `uh`, `entradas`, `saidas` |
| `ouro_reloginho_indicadores` / `_frente` | `uh_contratadas/entregues/vigentes` |
| `ouro_reloginho_indicadores_entregas` | `uh_entregues_evento_mes/_acum`, `n_eventos`, `uh_entregues_snapshot`, `dif_evento_vs_snapshot` |
| `prata_dhist_snh_entregas_mes` | `uh_entregues_evento_mes` |

Verificado pós-rebuild: `HUGEINT` = 0 em todos; somas nacionais inalteradas
(`ouro_dhist_serie_mensal` Σ uh_contratadas nacional = 120.606.220, = silver;
`ouro_reloginho_indicadores` CAIXA 2026-03 = 1.697.630 / 1.391.909 = ref #66).
`ouro_reloginho_resumo_dashboard` herda `bigint` do upstream, sem edição.

## Itens 2–4 — inventário + `verificacao_tipagem`

Inventário e tabela canônica: `inventario-tipagem-silver-gold.md`.

`verificacao_tipagem` reescrita (comparação por string exata mantida; resolve a
tabela por `model.schema`/`model.identifier`, sem schema hard-coded). `schema.yml`
das colunas-chave de:

- `mcmv_historico_dbt` silver (3 por-frente via anchor + entrega_apf + obra_mensal
  + serie_executiva) e gold (serie_mensal, serie_situacao_mensal,
  marco_empreendimento, snapshot_empreendimento_atual);
- `indicadores_mcmv_dbt` silver (snh_apf_mes, snh_entregas_mes) e os 4 golds do
  reloginho.

`tipo_esperado` nos nomes do DuckDB (modo A). **Todos PASS** no `dbt test`
local dos dois domínios (`--no-partial-parse`): `PASS=435 WARN=42 ERROR=1`, o
único ERROR é o pré-existente `assert_reloginho_frente_cobertura_mensal`
(lacuna de FONTE). Fora de escopo: os 2 golds de gargalo.

> Após editar `schema.yml`, rode `dbt test` com `--no-partial-parse` na
> primeira vez — o partial-parse do dbt às vezes não recolhe testes novos de
> um `schema.yml` alterado junto com outros (visto nesta change: os 20 testes
> de gold só apareceram no parse limpo).

Numa ida futura a Postgres os `tipo_esperado` precisam de revisão (nomes de
`data_type` mudam) — anotado no macro e em `convencao-testes-qualidade.md`.
