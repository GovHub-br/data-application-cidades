# Entrega — `auditar-grao-serie-executiva-historica`

Data: 2026-09-07. Escopo: **build local (`staging_duckdb`)**, nada promovido a
prod. Change OpenSpec `auditar-grao-serie-executiva-historica`.

## Problema

`silver_mcmv_historico_serie_executiva` tratava as 4 famílias pré-2019 como
snapshot por empreendimento e deduplicava com `row_number() … rn = 1`, mantendo
**uma linha de movimento arbitrária** por `(chave_natural, dt_referencia)`.
`bases_relatorio_executivo` e `bext` são, na verdade, **razões de movimento de
unidades** (coluna `uh` assinada: `+N` contratação, `−N` distrato) restatados a
cada arquivo mensal. Consequência: 4.859 linhas com UH negativa, faixa /
subsídio / `linha_ogu_fgts` da linha sobrevivente sorteados, milhares de linhas
com `uh_entregues > uh_contratadas`.

## Auditoria (doc completo: `models/mcmv_historico_dbt/docs/auditar-grao-serie-executiva-bases-relat-exec.md`)

| Família | grão real | `grao_familia` | `natureza_serie` | tratamento |
|---|---|---|---|---|
| `bases_relatorio_executivo` | empreendimento (APF) — razão de movimento | `empreendimento` | `estoque` | dedup reenvio → dedup conteúdo → `SUM` por (APF, mês) |
| `min_cidades` | contrato PF (1 linha/contrato/mês) | `contrato` **(era `empreendimento`)** | `estoque` | idem, grão contrato |
| `bext` | contrato PF — mesmo padrão `+N`/`−N` | `contrato` | `estoque` | idem, grão contrato |
| `entrada_bb` | empreendimento BB | `empreendimento` | `fluxo` **(era `estoque` fixo no gold)** | dedup reenvio → dedup conteúdo |

**Decisão sobre `+N`/`−N`** (Open Question 2): são **movimento** (distrato), não
artefato. Mas 87% dos grupos APF multi-linha na mesma faixa são **reingestão
byte-a-byte** da mesma linha de negócio (o extrator a montante emite `"295.0"` e
`"295"`). Fluxo na silver, nesta ordem:

1. `reenvio_rank` — `dense_rank` por `(fonte_familia, chave, faixa, município,
   mês)` ordenado por `report_date_parsed desc, source_file desc`; mantém só o
   `source_file` mais recente (`_v2` > base). Antes de qualquer soma.
2. `conteudo_rank` — `row_number` por `(fonte_familia, chave, conteudo_hash,
   mês)`; colapsa a reingestão idêntica.
3. `agregado` — `SUM` por `(fonte_familia, coalesce(chave, conteudo_hash),
   mês)`. Os pares `+N`/`−N` genuínos netam (distrato → 0). `linha_ogu_fgts` e
   `situacao_derivada` recalculados sobre os valores agregados.

`natureza_serie` = coluna nova da silver, propagada ao `gold_serie_mensal`
(removido o literal `'estoque'`).

## Diff de números

### Silver `silver_mcmv_historico_serie_executiva` — totais por família

| família | linhas (=) | `Σ uh_contratadas` antes → depois | `Σ uh_comercializadas` antes → depois |
|---|---|---|---|
| `bases_relatorio_executivo` | 783.814 | 97.078.285 → 97.065.471 (−0,01%) | 62.344.692 → 62.344.840 |
| `min_cidades` | 3.759.282 | 7.710.209 → 7.735.064 (+0,3%) | 1.622.760 → 1.632.102 |
| `bext` | 5.554.442 | 13.532.583 → 13.532.609 | — |
| `entrada_bb` | 9.072 | 2.169.180 → 2.273.076 (+4,8%) | — |

- `bases_relatorio_executivo`: queda pequena (net dos distratos `+N`/`−N` que
  antes contavam como `+N`).
- `min_cidades` / `entrada_bb`: **aumento** — o `rn = 1` antigo, com desempate
  `hash_linha`, escolhia arbitrariamente entre a linha com métrica e a linha
  gêmea sem métrica (`SUM` recupera o valor real). Correção, não regressão.

### Qualidade (`bases_relatorio_executivo`, silver inteira)

| checagem | antes | depois |
|---|---|---|
| `uh_contratadas < 0` | 4.881 | **0** |
| `uh_entregues > uh_contratadas` | ~7.300 | 2.472 |
| `uh_concluidas > uh_contratadas` | 4.753 | **1** |
| `uh_comercializadas > uh_entregues` | 186.345 | 186.345 — característica da fonte (venda antecede entrega), não aninhada |

Negativos de UH nas 4 famílias: `bext` 253 → **0**, demais já 0.

### Gold `gold_serie_mensal`

Contrato de colunas: **prefixo idêntico**; `natureza_serie` deixa de ser fixo;
`uh_comercializadas` adicionado como **última coluna**. `uh_em_obras` (já no
contrato) mantido na posição.

Como o gold nacional = soma da silver por família/mês, o delta do gold por
`(mês, fonte_familia, nível)` é o mesmo da silver acima. Spot check
`bases_relatorio_executivo` / nacional / 2018-08: `uh_contratadas`
3.005.577 → 3.001.213; `uh_comercializadas` (nova) = 2.140.036. Magnitude por
mês na casa de 1–2 M (estoque de um mês), nunca a soma plurianual — conforme a
spec.

## Impacto nos consumidores

- **Piloto #118 / backtest do relógio**: os totais mensais de UH mudam ≤0,3%
  em `bases_relatorio_executivo` (a família que o piloto usa); o corte por
  `linha_ogu_fgts` fica mais confiável (derivado dos subsídios somados do APF, não
  de uma linha sorteada). `entrada_bb` sobe 4,8% mas é família de `fluxo`, fora
  do piloto.
- Nenhuma mudança de contrato quebra leitura posicional: só adição ao fim.

## Testes DQ (todos `warn`, não filtram linha)

`silver_mcmv_historico_serie_executiva` — `dbt test`:

| teste | resultado |
|---|---|
| `valor_nao_negativo` × `uh_contratadas/entregues/concluidas/em_obras/comercializadas` | **PASS** (0 negativos após a correção) |
| `quantidade_nao_excede_referencia` (ref `uh_contratadas`) × `uh_entregues` | WARN 2.472 |
| … × `uh_concluidas` | WARN 1 |
| … × `uh_em_obras` | PASS |
| … × `uh_comercializadas` | WARN 2.701 |
| `reconcilia_decomposicao` (financeiro, pré-existente) | WARN 1.310 (era ~6,5 k) |
| `valor_dentro_faixa_uh` (pré-existente) | WARN 632 |

`gold_serie_mensal`: `soma_nao_cruza_familia` WARN 513 (janela de sobreposição
`min_cidades`×`bases_relatorio_executivo` 2014–2016 — característica do dado, o
teste é lembrete). `accepted_values` de `natureza_serie` (`estoque`/`fluxo`) e
`grao_familia` (`contrato`/`empreendimento`): PASS.

## Open Questions residuais

1. **Decomposição por faixa**: não exposta (nenhum APF tem faixas aditivas
   legítimas no período). Se um mart futuro precisar do corte por faixa dentro
   do APF, é um modelo lateral.
2. **`entrada_bb` +4,8%**: confirmado como correção (recuperação de métrica que
   o `rn=1` descartava), não dupla contagem — família pequena (9 k linhas), de
   `fluxo`, sem consumidor crítico.
3. **`uh_comercializadas > uh_entregues` (24%)**: característica da fonte
   (comercializada = vendida/reservada, antecede a entrega). Documentado; o
   teste de aninhamento usa `uh_contratadas` como referência, não `uh_entregues`.
