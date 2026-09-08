# Entrega — vocabulário e qualidade financeira do eixo histórico

Change `vocabulario-e-qualidade-financeira-historica`. Build local
(`staging_duckdb`, `/mnt/data/duckdb/cidades.duckdb`), nunca Postgres.
Docs de referência: `../glossario-valores-financeiros.md`,
`../varredura-colunas-financeiras-historico.md`.

## Adendo pós-revisão — alinhamento com os modelos dos colegas (prod)

Antes de fechar a change, varredura de como as fichas atuais
(`empreendimento_far/fds/rural_dbt`, `entidades_dbt`, `mcmv_silver_dbt`) tratam
valores financeiros. Dois ajustes adotados por consistência (detalhe no §9 do
glossário):

1. **Tipo `numeric(15,2)`** — `serie_executiva` e as silvers por frente
   históricas passaram de `double` (`parse_hist_double`) para `numeric(15,2)`
   (`parse_hist_numeric`), o mesmo tipo de `parse_financial_value` das fichas.
   `sum()` exato no gold, sem erro de ponto flutuante. Reconstruído local:
   contagens dos testes idênticas (nenhuma regressão). Percentuais seguem
   `double`.
2. **Vocabulário** — `valor_emprestimo` → `valor_financiamento` (par de
   `valor_financiamento_fds` / `valor_far`); `valor_contrapartida` →
   `valor_contrapartidas` (plural, par de `valor_contrapartidas`). Vale para a
   silver e para o `gold_serie_mensal` (`valor_emprestimo_acumulado` →
   `valor_financiamento_acumulado`).

A semântica de ausência **diverge de propósito** das fichas: o histórico mantém
`NULL` para "sem informação"; as fichas fazem `coalesce(...,0.00)`. O histórico
está mais correto e a reconciliação de somas confirma que nenhuma agregação
regride.

## Reconciliação — `silver_mcmv_historico_serie_executiva`

Baseline: silver materializada em 2026-09-06 antes da change.

| família | linhas antes | linhas depois | Δ | chaves antes | chaves depois | motivo do Δ |
|---|---:|---:|---:|---:|---:|---|
| `bases_relatorio_executivo` | 787.215 | 783.782 | −3.433 (−0,44%) | 26.125 | 26.020 | quarentena (−105 chaves) |
| `bext` | 5.607.857 | 5.554.442 | −53.415 (−0,95%) | 297.923 | 294.214 | quarentena (−3.709 chaves) |
| `entrada_bb` | 9.072 | 9.072 | 0 | 746 | 746 | — |
| `min_cidades` | 3.759.282 | 3.759.282 | 0 | 255.856 | 255.856 | — |

Grão preservado: a dedup por `(fonte_familia, chave_natural, dt_referencia)` não
mudou; o Δ é só a exclusão de chaves inteiras pela quarentena (3.814 chaves no
seed; ~3.814 aparecem na série executiva). **Não há multiplicação de linhas.**

### Colunas de valor — somas por família (R$ bi nominais, todos os snapshots)

> São somas de série de **estoque** com multiplicação de snapshot — servem só
> para comparar antes/depois, **não** são totais de programa.

| família | `valor_investimento` | `valor_financiamento` | `valor_vgv` | `valor_contrapartidas` | `valor_liberado` |
|---|---:|---:|---:|---:|---:|
| `bases_relatorio_executivo` | 8.751 (era 8.779) | 6.019 (era 6.032) | — | **2.732 (nova)** | 2.921 |
| `bext` | 793 (era 858) | 785 (era 832) | — | **9,3 (nova)** | 118 |
| `entrada_bb` | — | **94 (era 246)** | **246 (nova)** | — | — |
| `min_cidades` | 279 | 725 | — | **11,9 (nova)** | — |

Mudanças de vocabulário:

- **VGV fora de `valor_financiamento`** (D2): `entrada_bb` tinha R$ 246 bi de VGV
  rotulados como empréstimo. Agora `valor_vgv` = 246 bi e `valor_financiamento` =
  94 bi (de `total_financiamentos_pf`, o financiamento PF real).
- **Contrapartida resgatada** (D2): `mvalor_contrapartida_poder_publico` (bext) e
  as colunas de contrapartida das outras famílias, antes descartadas, agora em
  `valor_contrapartidas` (62,9% de preenchimento).
- **`grao_familia`** (D3): `bext` = `contrato`, demais = `empreendimento`.
- **Quarentena** (D6): 0 registros com valor financeiro negativo restam na
  silver (eram ~26 mil linhas / 2.539 chaves); outlier `bext/2694194`
  (R$ 2,4 bi / 128 UH) removido.

Testes novos no `schema.yml` da silver: `completude_minima` (warn) nas 8 colunas
de valor + `accepted_values` em `grao_familia`. Todos PASS (`dbt build` local,
2026-09-06).

## `gold_serie_mensal` — BREAKING (Fase 4)

| coluna antiga | coluna nova | conceito canônico |
|---|---|---|
| `valor_investimento` | `valor_investimento_acumulado` | `valor_investimento_total` |
| `valor_financiamento` | `valor_financiamento_acumulado` | `valor_financiamento` |
| `valor_liberado` | `valor_desembolsado_acumulado` | `valor_desembolsado_acumulado` |

Colunas novas: `natureza_serie` (constante `'estoque'`), `grao_familia`
(`contrato`/`empreendimento`), `valor_vgv`, `valor_contrapartidas`,
`subsidio_total`. Demais colunas e tipos preservados.

Build local 2026-09-06: 6.251 linhas, `natureza_serie` = `estoque` em 100%.
Testes: `cobertura_classificacao_ogu_fgts` PASS (cobertura nacional 75,6% ≥
piso 0,60); `soma_nao_cruza_familia` **WARN 513** — esperado: 513 `(mês, uf)`
com `bext` (contrato) + família de empreendimento na janela de sobreposição
2014-2018. É lembrete, não bug (design D3/D7).

### Consumidores a avisar (task 5.5)

Nenhum modelo dbt nem arquivo em `superset/` deste repo lê as colunas antigas
por nome (`gold_serie_mensal` é gold-folha). O BREAKING atinge **consumidores
externos** (dashboards / notebooks / OpenMetadata que leiam
`serie_historica.gold_serie_mensal`, hoje `dados_historicos.gold_serie_mensal`). A mudança de contrato está registrada no
`schema.yml`, no glossário e aqui; a notificação aos donos de dashboard é passo
de processo fora do dbt. Precedente: a spec `serie-historica-regiao` já mudou o
contrato deste modelo.

## Fonte canônica de desembolso (Fase 5)

- **6.1** `bronze_far/rural/fds_financeiro_mensal`: `schema.yml` + header SQL
  agora afirmam "snapshot único do SharePoint, `dt_referencia` constante,
  liberações desde 2024-06/07, feed de decomposição por componente".
- **6.2** `silver_far/fds/rural_evolucao_financeira` (+ `entidades_dbt`): doc
  deixa de dizer "série temporal de desembolsos" — "liberações conhecidas no
  snapshot, agrupadas pelo mês da liberação, cobertura pós-2024".
- **6.3** `gold_indicadores_gargalo_desempenho`: `valor_liberado_historico` =
  `valor_desembolsado` da ficha (sem `coalesce` do agregado SharePoint); nova
  coluna informativa `valor_desembolsado_componentes`.
- **6.4** Distribuição dos flags **não mudou** (FAR gargalo 763/822, baixa_exec
  601/822 — idênticos ao baseline). Os flags nunca consumiram
  `valor_liberado_historico`; a taxa de 93% vem dos limiares, não da fonte.
  Detalhe em `../../indicadores_mcmv_dbt/docs/gargalo-fonte-desembolso-flags.md`.
- **6.5** `tests/indicadores_mcmv_dbt/assert_reconcilia_fontes_desembolso.sql`
  (singular, `warn`) — Σ GEFUS × Σ SharePoint por frente, banda [0,1; 10].
  PASS no build local (as somas ficam na mesma ordem de grandeza no
  subconjunto com as duas fontes).

## Componentes do FDS (Fase 6)

- **7.1** Varredura do `s3://data-lake-mcid/staging/sharepoint/novo_mcmv_fds_financeiro_mensal.parquet`:
  **não há coluna de componente não mapeada** — a bronze já mapeia os 8
  `vr_pago_*` que existem. A cobertura de 19% vem de o feed **não decompor**
  ~80% das linhas (12.857/15.977 com todos os componentes NULL, sobretudo
  `co_tipo_movimento = 1`).
- **7.2** Residual explícito `vr_pago_outros_mes` = `greatest(vr_liberado_mes −
  Σ conhecidos, 0)` nas duas silvers FDS (`empreendimento_fds_dbt` e
  `entidades_dbt`) e no Rural (= atec + cisternas + custos indiretos).
- **7.3** `reconcilia_decomposicao` (`warn`, tol R$ 1,00) nas silvers de
  liberação FAR/FDS/Rural. Build local 2026-09-06:
  - FAR: **PASS** — `Σ componentes = vr_liberado` sem residual (comportamento
    preservado).
  - Rural: **PASS** — fecha por construção (o total é bottom-up dos componentes).
  - FDS: **WARN 59** — 59/245 linhas (24%) têm `Σ componentes > vr_liberado`
    (excesso agregado R$ 1,85 mi). O feed tem sub- **e** super-decomposição; o
    `warn` é o sinal de que ele é pouco confiável para composição, não um bug
    do modelo.
- **7.4** Consolidação das 2 pipelines FDS `evolucao_financeira`: **adiada**
  (Open Question 4). Motivo: leem fontes/dialetos distintos
  (`source('raw')` Postgres + `parse_financial_value`→`0.00` vs staging MinIO +
  `parse_hist_numeric`→`NULL`); a `entidades_dbt` é produto de outro time e não
  materializa no `staging_duckdb`. Feito agora: as duas silvers ganharam a
  coluna `vr_pago_outros_mes` (paridade), os headers/doc foram alinhados, e a
  divergência de parser está registrada no `schema.yml` da bronze
  `entidades_dbt.fds_financeiro_mensal`. Consolidação física → change própria.

## Reconciliação de decomposição + testes de valor (Fase 7)

- **8.1** `reconcilia_decomposicao` na `serie_executiva`
  (`valor_investimento = valor_financiamento + valor_contrapartidas + subsidio_total`, `warn`,
  tol R$ 100 k): `bases_relatorio_executivo` **fecha exato** (R$ 0);
  `bext` tem gap sistemático ~R$ 16-25 k/linha, `min_cidades` ~R$ 26-51 k/linha
  (o investimento inclui taxas fora das 3 parcelas). **WARN 1.275** (pares
  distintos; ~6,5 k linhas > R$ 100 k, sobretudo `min_cidades` > R$ 1 M).
- **8.2** `desembolso_nao_excede_contratado` nas 3 silvers por frente:
  FAR **WARN 9** (máx 1,41×), FDS **PASS** (0 APF), Rural **WARN 107** +
  o gate `error` (razão > 2) **PASS** após os 7 APF entrarem na quarentena
  (`fonte_familia = 'rural_historico'`).
- **8.3** `valor_nao_negativo`: `error` nas 8 colunas de valor da
  `serie_executiva` (**PASS** — 0 negativos pós-quarentena; a dedup passou a
  desempatar preferindo a linha sem valor negativo quando a chave/mês tem as
  duas versões); `warn` em `valor_contratado`/`valor_desembolsado` das silvers
  por frente (**FAR WARN 26** — negativos de snapshot pontual, que a quarentena
  por chave não deve remover) e nos bronzes `serie_bext` /
  `serie_bases_relatorio_executivo` (cópia fiel).
- Anti-join à quarentena adicionado às 3 silvers por frente (D6) —
  `fonte_familia ∈ {far_historico, fds_historico, rural_historico}`.
- Toggle de dev `--vars 'quarentena_bypass: true'` na `serie_executiva` para
  re-varrer o seed sem circularidade.

## Verificação final (Fase 9)

- **9.1** `openspec validate vocabulario-e-qualidade-financeira-historica --strict` → **valid**.
- **9.2** `dbt build` consolidado dos modelos afetados no `staging_duckdb`,
  2026-09-06: **PASS 139 / WARN 17 / ERROR 0** (6 dos WARN são
  `sem_sufixo_float_texto` pré-existentes da change `testes-data-quality-dbt`).
  Os testes financeiros novos:

  | teste | resultado | nota |
  |---|---|---|
  | `valor_nao_negativo` (serie_executiva, 8 col, `error`) | PASS | 0 negativos pós-quarentena |
  | `valor_nao_negativo` (bronze bext/bases_relatorio, `warn`) | WARN | cópia fiel |
  | `valor_nao_negativo` (FAR `valor_contratado`, `warn`) | WARN 26 | negativos de snapshot isolado |
  | `reconcilia_decomposicao` (serie_executiva, `warn`) | WARN 1.275 | gap bext/min_cidades ~R$ 25-50 k |
  | `reconcilia_decomposicao` (FAR/Rural liberação) | PASS | fecham |
  | `reconcilia_decomposicao` (FDS liberação, `warn`) | WARN 59 | feed sub+super-decompõe |
  | `desembolso_nao_excede_contratado` (FAR/Rural `warn`) | WARN 9 / WARN 107 | |
  | `desembolso_nao_excede_contratado` (Rural `error`, ratio>2) | PASS | 7 APF na quarentena |
  | `desembolso_nao_excede_contratado` (FDS) | PASS | 0 APF |
  | `cobertura_classificacao_ogu_fgts` (gold_serie_mensal, `warn`) | PASS | cobertura 75,6% ≥ 0,60 |
  | `soma_nao_cruza_familia` (gold_serie_mensal, `warn`) | WARN 513 | janela bext × empreendimento |
  | `completude_minima` (8 col de valor, `warn`) | PASS | |
  | `valor_dentro_faixa_uh` (serie_executiva `valor_investimento`, `warn`) | WARN 667 | seed provisório (Risks) |
  | `valor_nao_negativo` (bronze bext `mvalor_investimento`/`mvalor_subsidio`, `warn`) | WARN 1934 / 950 | cópia fiel |
  | `assert_reconcilia_fontes_desembolso` (singular, `warn`) | PASS | somas na mesma ordem de grandeza |

- **9.3** Este documento (reconciliação por família/frente nas seções acima).
- **9.4** `dbt docs generate` → `catalog.json` + `manifest.json` OK. Os 7 testes
  financeiros novos entram no `manifest.json` (277 `valor_nao_negativo`,
  67 `reconcilia_decomposicao`, 47 `desembolso_nao_excede_contratado`,
  24 `valor_dentro_faixa_uh`, 20 `soma_nao_cruza_familia`,
  19 `cobertura_classificacao_ogu_fgts` + `reconcilia_fontes_desembolso`
  singular). OpenMetadata os ingere pela ingestão de manifest existente, sem
  job novo.
- **9.5** Memória do projeto atualizada
  (`valores-financeiros-historico-auditoria-e-proposta`); cruza com a change
  `testes-data-quality-dbt` (task 6.5 "reconciliação de somas" ≡ o glossário +
  esta entrega).
