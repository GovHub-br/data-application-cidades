# Glossário canônico de valores financeiros — eixo histórico MCMV

> Change `vocabulario-e-qualidade-financeira-historica`.
> Complementa `glossario-mcid.md` (semântica geral) com o vocabulário
> **financeiro** do eixo histórico (série executiva pré-2019 + silvers por
> frente + golds). Varredura das colunas físicas:
> `varredura-colunas-financeiras-historico.md`.

## 1. Princípio

**Um conceito financeiro → um nome canônico.** As *silvers por frente*
(`silver_mcmv_historico_empreendimento_far/fds/rural`) mantêm o nome próximo da
fonte (`valor_contratado`, `valor_desembolsado`) — renomear em cascata é caro e
a silver por frente é legitimamente "vocabulário da fonte" — mas o `schema.yml`
de cada uma **declara a qual conceito canônico a coluna corresponde**. A
`silver_mcmv_historico_serie_executiva` e os *golds* (`gold_serie_mensal`,
`gold_snapshot_empreendimento_atual`, `gold_indicadores_gargalo_desempenho`)
usam o nome canônico.

**Alinhamento com as fichas atuais** (frentes FAR/FDS/Rural já em produção): o
vocabulário e os tipos desta change seguem os modelos dos colegas —
`valor_financiamento` (par de `valor_financiamento_fds` / `valor_far`),
`valor_contrapartidas` no plural (par de `valor_contrapartidas`), e todo valor
monetário em `numeric(15,2)` (via `parse_hist_numeric`, o mesmo tipo de
`parse_financial_value` das fichas). Ver §9.

## 2. Conceitos canônicos

| conceito canônico | definição de negócio | natureza |
|---|---|---|
| `valor_investimento_total` | custo total do empreendimento/contrato = financiamento + contrapartidas + subsídio | estoque |
| `valor_financiamento` | operação de crédito (empréstimo / financiamento à produção), **sem VGV** | estoque |
| `valor_vgv` | valor global de venda das unidades habitacionais | estoque |
| `valor_contrapartidas` | aporte de estado / município / entidade organizadora | estoque |
| `subsidio_fgts` | subsídio / complemento com fonte FGTS | estoque |
| `subsidio_ogu` | subsídio / complemento com fonte OGU (Orçamento Geral da União) | estoque |
| `subsidio_total` | subsídio sem split por fonte (só a família `bext` reporta assim) | estoque |
| `valor_desembolsado_acumulado` | recurso federal efetivamente liberado, acumulado até o snapshot | estoque |
| `valor_desembolsado_mes` | liberação do mês (movimento do período) | fluxo |
| `valor_desembolsado_componente_*` | decomposição da liberação (obra, terreno, PTS, INCC, projeto, legalização, aporte, …) | fluxo |
| `valor_desembolsado_componente_outros` | resíduo explícito quando a fonte não fornece todos os componentes | fluxo |

**Regra de subsídio:** `subsidio_fgts + subsidio_ogu + subsidio_total` é dupla
contagem. A leitura correta é `coalesce(subsidio_fgts + subsidio_ogu, subsidio_total)`.

## 3. Crosswalk — coluna física → conceito, por camada e frente

### Série executiva (`silver_mcmv_historico_serie_executiva`)

| conceito | `bases_relatorio_executivo` | `min_cidades` | `entrada_bb` | `bext` |
|---|---|---|---|---|
| `valor_investimento_total` | `valor_total_do_investimento` | `vlr_total_operacao` \| `vlr_total_investimento` | — | `mvalor_investimento` |
| `valor_financiamento` | `valor_do_emprestimo` | `vlr_financiamento` \| `vlr_emprestimo` | `total_financiamentos_pf` \| `total_financ_a_producao_pj` | `mvalor_emprestimo` \| `mvalor_financiamento` |
| `valor_vgv` | — | — | `valor_global_de_venda_vgv` | — |
| `valor_contrapartidas` | `valor_contrapartida_poder_publico` \| `valor_da_contrapartida_do_poder_publico` | `vlr_contrapartida` | — | `mvalor_contrapartida_poder_publico` |
| `subsidio_fgts` | `subsidio_fgts` \| `siaci_valorsubsidio_fgts` | `vlr_subsidio_fgts` | `valor_fgts_pf_utilizado` | — |
| `subsidio_ogu` | `subsidio_ogu` \| `siaci_valorsubsidio_ogu` | `vlr_subsidio_ogu` | — | — |
| `subsidio_total` | — | — | — | `mvalor_subsidio` |
| `valor_desembolsado_acumulado` | `valor_total_liberado` | `vlr_total_liberado` (raro) | — | `mvalor_desembolso` |

`\|` = coalesce entre gerações de schema. `—` = a fonte não tem o conceito.

### Silvers por frente (SFTP GEFUS/INT ∪ SNH)

| conceito | FAR (`silver_mcmv_historico_empreendimento_far`) | FDS (`_fds`) | Rural (`_rural`) |
|---|---|---|---|
| `valor_investimento_total` | col. `valor_contratado` ← `vr_investimento` (INT040/054) / `valor_contratado` (SNH) | col. `valor_contratado` ← `vr_investimento` (INT059) / `valor_contratado` (SNH) | col. `valor_contratado` ← `vr_investimento` (INT057) / `vr_investimento_pnhr` (INT065) / `valor_contratado` (SNH) |
| `valor_desembolsado_acumulado` | col. `valor_desembolsado` ← `vr_liberado` (INT040) / `total_liberado_far` (INT054) / `valor_desembolsado` (SNH) | col. `valor_desembolsado` ← `vr_liberado` (INT059) / `valor_desembolsado` (SNH) | col. `valor_desembolsado` ← `vr_liberado` (INT065/INT057) / `valor_desembolsado` (SNH) |
| `valor_financiamento` | `vr_emprestimo_far` (INT054) — **não propagado** | `vr_emprestimo_original` (INT059) — **não propagado** | `vr_emprestimo` / `vr_emprestimo_siapf` (INT065) — **não propagado** |
| `valor_contrapartidas` | `vr_contrapartida_1` — **não propagado** | `contrapartida_financeira` + `contrapartida_servicos` + `contrapartida_poder_pub_local_*` — **não propagado** | `vr_contrapartida` — **não propagado** |

### Golds

| conceito | `gold_serie_mensal` | `gold_snapshot_empreendimento_atual` | `gold_indicadores_gargalo_desempenho` (schema `reloginho`) |
|---|---|---|---|
| `valor_investimento_total` | `valor_investimento_acumulado` *(era `valor_investimento`)* | `valor_contratado` | `valor_contratado` |
| `valor_financiamento` | `valor_financiamento_acumulado` *(era `valor_emprestimo`)* | — | — |
| `valor_desembolsado_acumulado` | `valor_desembolsado_acumulado` *(era `valor_liberado`)* | `valor_desembolsado` | `valor_desembolsado` (ficha GEFUS/CAIXA — **primário**) |
| `valor_vgv` | `valor_vgv` (nova) | — | — |
| `valor_contrapartidas` | `valor_contrapartidas` (nova) | — | — |
| `subsidio_total` | `subsidio_total` (nova) | — | — |
| `valor_desembolsado_componente_*` | — | — | `valor_desembolsado_componentes` (agregado SharePoint — **informativo**, cobertura parcial pós-2024) |

**Mapa nome-antigo → nome-novo (`gold_serie_mensal`, BREAKING):**

| antes | depois |
|---|---|
| `valor_investimento` | `valor_investimento_acumulado` |
| `valor_emprestimo` | `valor_financiamento_acumulado` |
| `valor_liberado` | `valor_desembolsado_acumulado` |

Colunas novas no `gold_serie_mensal`: `natureza_serie`, `grao_familia`,
`valor_vgv`, `valor_contrapartidas`, `subsidio_total`.

## 4. Fonte canônica de desembolso (D4)

- **Acompanhamento histórico** (silvers por frente, `gold_snapshot`,
  `gold_serie_mensal`): desembolso acumulado autoritativo = feed GEFUS/INT
  (`vr_liberado` / `total_liberado_far` das INT040/054/057/059/065) e, pré-2019,
  `valor_total_liberado` de `bases_relatorio_executivo`. Snapshot cumulativo da
  fonte oficial.
- **Decomposição por componente** = `*_financeiro_mensal` (SharePoint).
  É **detalhe**, não total, e cobre só APF com liberação pós-2024.
- `gold_indicadores_gargalo_desempenho` **não** faz mais
  `coalesce(<sharepoint>, <ficha>)`: a ficha (GEFUS/CAIXA) é primária; o
  agregado SharePoint vira coluna informativa `valor_desembolsado_componentes`.

## 5. Natureza de série — estoque × fluxo

`gold_serie_mensal` é **série de estoque**: cada linha é a carteira acumulada no
mês-snapshot (coluna `natureza_serie = 'estoque'`). **Não somar entre meses**
(dupla contagem do acumulado) **nem entre `fonte_familia`** de grão diferente
(`grao_familia`: `'contrato'` para `bext`, `'empreendimento'` para as demais).
Para montar uma série contínua: filtrar por `prioridade_familia` (menor =
preferencial) por `(dt_referencia, uf)`. Guardas: testes
`soma_nao_cruza_familia` e `cobertura_classificacao_ogu_fgts` (D7).

Os `*_financeiro_mensal` e os `silver_*_evolucao_financeira` são **fluxo**
(liberação agrupada pelo mês da liberação), cobertura pós-2024.

## 6. Limitação — valores nominais (D8)

Todos os valores estão em **R$ nominais** (moeda da data do fato, 2012–2026).
Comparação plurianual exige deflator externo (IPCA, INCC ou IGP-M, a definir com
negócio). **Não** há coluna `_deflacionado` nesta change.

## 7. Nota — backtest OGU/FGTS (piloto #118)

`linha_ogu_fgts` classifica pelo subsídio dominante. A família `bext` reporta só
`subsidio_total` (sem split FGTS/OGU) → todo o `bext` fica
`linha_ogu_fgts = 'Nao classificada'` no `gold_serie_mensal`. Cobertura nacional
de `valor_investimento_acumulado` classificado observada em 2026-09-06: **75,6%**
(`bases_relatorio_executivo` domina o investimento e traz o split; `bext` puxa
para baixo). O teste `cobertura_classificacao_ogu_fgts(min_pct=0.60)` sinaliza
regressão em `warn`; a derivação do split para `bext` (Open Question 3) fica
pendente de negócio.

## 8. Pendências de negócio (tarefa 1.3 — registradas, não confirmadas)

| # | pergunta | assunção atual (design) |
|---|---|---|
| OQ1 | `valor_contratado` nas silvers por frente → renomear para `valor_investimento` ou manter? | **Manter + documentar** — e nas fichas atuais `valor_contratado` também é o investimento total (FAR + contrapartidas), então o nome já é comum aos dois eixos. `serie_executiva` alinhou o resto do vocabulário (`valor_financiamento`, `valor_contrapartidas` — ver §9). |
| OQ2 | Faixa R$/UH: por frente só, ou por frente × faixa × ano? | **Por frente × faixa** (`faixa_valor_uh.csv`), sem recorte de ano; calibrado na distribuição observada, revisável. |
| OQ3 | `bext` `linha_ogu_fgts`: derivar de `faixa`/`produto` quando o split não vem, ou assumir `'FGTS/Financiado'` (base extrato CAIXA)? | **Deixar `NULL`** e sinalizar via `cobertura_classificacao_ogu_fgts`; derivação adiada. |
| OQ4 | Consolidação das 2 pipelines FDS `evolucao_financeira` entra nesta change? | **Não** — adiada para change própria. Fontes/dialetos distintos (`raw` Postgres + `parse_financial_value`→`0.00` vs staging MinIO + `parse_hist_numeric`→`NULL`); `entidades_dbt` é produto de outro time e não roda no `staging_duckdb`. Feito: paridade de coluna (`vr_pago_outros_mes`), headers alinhados, divergência de parser registrada no `schema.yml`. |
| OQ5 | Gap 2018-09 → 2019-11: alguma família do dump cobre parte? | Fora do escopo confirmar; registrado como limitação. |

## 9. Tipo e semântica de ausência — alinhamento com as fichas atuais

Levantamento (2026-09-06) de como os modelos dos colegas já em produção
(`empreendimento_far/fds/rural_dbt`, `entidades_dbt`, `mcmv_silver_dbt`) tratam
valores financeiros, e o que o eixo histórico adotou por consistência:

| aspecto | fichas atuais (prod) | eixo histórico (esta change) |
|---|---|---|
| **tipo** | `numeric(15,2)` em tudo (`parse_hist_numeric` / `parse_financial_value`) | **`numeric(15,2)`** — `parse_hist_numeric` em `serie_executiva` e nas silvers por frente (era `double` via `parse_hist_double`). `sum()` exato no gold; sem erro de ponto flutuante. |
| **ausência na silver** | `coalesce(valor, 0.00)` — "sem informação" e "R$ 0" viram o mesmo | **`NULL` preservado** — `parse_hist_numeric` devolve `NULL` p/ vazio/`None`/`NaN` (comportamento medalhão correto). Divergência **deliberada**: mais informativa; a reconciliação de somas confirma que nenhuma agregação regride. |
| **empréstimo/financiamento** | `valor_financiamento_fds` (FDS/Rural silver), `valor_far` (FAR bronze) | `valor_financiamento` — mesmo conceito, sem sufixo de frente (a série executiva mistura FAR/FDS/Rural). |
| **contrapartida** | `valor_contrapartidas` (plural) | `valor_contrapartidas` (plural) — idêntico. |
| **desembolso FDS/Rural** | `sum(abs(vr_liberado))` com `ic_credito='0'` (feed SharePoint, negativos = liberação) | feed distinto (INT059/INT065 SFTP), já positivo/acumulado — convenção própria, legítima. |
| **contrato cross-frente** | `silver_historico_base` carrega só `valor_contratado` + `valor_desembolsado` | os golds históricos carregam mais conceitos; um rollup para aquele contrato mapeia `valor_investimento(_acumulado)`→`valor_contratado`, `valor_desembolsado_acumulado`→`valor_desembolsado`. |

Percentuais (`percentual_execucao_fisica`) seguem em `double` — não são dinheiro.
