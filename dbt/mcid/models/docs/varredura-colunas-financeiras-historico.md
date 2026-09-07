# Varredura de colunas financeiras — eixo histórico

> Change `vocabulario-e-qualidade-financeira-historica`, tarefa 1.1.
> Fonte: cópia de `/mnt/data/duckdb/cidades.duckdb` (2026-09-06), bronzes por
> família já materializadas. Números são de contagem de linhas na bronze
> (snapshot mensal — a mesma chave aparece em dezenas de meses).
>
> **Corrige** a seção "Mapa de colunas → contrato comum" de
> `entregas/issue-130-proposta-bronze-series-historicas.md`, que mistura
> conceitos: lá `valor_investimento` é mapeado de `valor_global_de_venda_vgv`
> (VGV, família `entrada_bb`) e de `valor_do_emprestimo` (empréstimo, `bext`).
> São conceitos distintos — ver o glossário
> (`glossario-valores-financeiros.md`).

## 1. Grão real de cada família da série executiva

| família | grão real | linhas bronze | chaves distintas | janela `dt_referencia` |
|---|---|---:|---:|---|
| `bases_relatorio_executivo` | empreendimento × mês | ~1,01 M | 26.125 | 2012-07 → 2018-08 |
| `min_cidades` | empreendimento/contrato × mês | ~3,90 M | 255.856 | 2014-10 → 2016-07 |
| `entrada_bb` | empreendimento (BB) × mês | ~18 k | 746 | 2012-10 → 2014-09 |
| `bext` | **contrato PF individual** × mês (`uh_contratadas = 1` por linha) | ~5,66 M | 297.923 | 2012-04 → 2018-08 |

`bext` **não** é "agregado UF×faixa×ano-mês" como diz o issue-130 — é o extrato
de contrato PF da CAIXA, uma linha por operação de financiamento.

## 2. Colunas financeiras por família — o que existe de fato

Fill = linhas não vazias / total de linhas na bronze.

### `bases_relatorio_executivo` (95 colunas)

| coluna física | fill | conceito canônico |
|---|---:|---|
| `valor_total_do_investimento` | ~100% | `valor_investimento_total` |
| `valor_do_emprestimo` | ~100% | `valor_financiamento` |
| `valor_total_liberado` | ~100% | `valor_desembolsado_acumulado` |
| `valor_contrapartida_poder_publico` | 72% | `valor_contrapartidas` (geração nova) |
| `valor_da_contrapartida_do_poder_publico` | 28% | `valor_contrapartidas` (geração antiga) |
| `subsidio_fgts` / `siaci_valorsubsidio_fgts` | 72% / 28% | `subsidio_fgts` |
| `subsidio_ogu` / `siaci_valorsubsidio_ogu` | 72% / 28% | `subsidio_ogu` |
| `valor_gestao_condominial`, `valor_trabalho_social_tts` | — | componentes (fora de escopo) |

As duas gerações de contrapartida/subsídio são **complementares** (uma preenche
onde a outra é nula): `coalesce()` das duas.

### `min_cidades` (102 colunas)

| coluna física | fill | conceito canônico |
|---|---:|---|
| `vlr_total_operacao` | 59% | `valor_investimento_total` (proxy) |
| `vlr_total_investimento` | <0,1% | `valor_investimento_total` (raro) |
| `vlr_financiamento` | ~99% | `valor_financiamento` |
| `vlr_emprestimo` | 0,6% | `valor_financiamento` (raro — não usar sozinho) |
| `vlr_total_liberado` | 0,6% | `valor_desembolsado_acumulado` (raro; hoje **não mapeado**) |
| `vlr_subsidio_fgts` / `vlr_subsidio_ogu` | ~99% | `subsidio_fgts` / `subsidio_ogu` |
| `vlr_contrapartida` | 0,6% | `valor_contrapartidas` (raro) |
| `vlr_recurso_proprio`, `vlr_sacado_fgts` | — | componentes (fora de escopo) |

### `entrada_bb` (56 colunas)

| coluna física | fill | conceito canônico |
|---|---:|---|
| `valor_global_de_venda_vgv` | ~99% | **`valor_vgv`** (antes da change caía errado no empréstimo) |
| `total_financiamentos_pf` | ~99% | `valor_financiamento` (financiamento PF) — hoje não mapeado |
| `total_financ_a_producao_pj` | ~99% | `valor_financiamento` (produção PJ) — hoje não mapeado |
| `valor_fgts_pf_utilizado` | ~99% | `subsidio_fgts` (proxy) — hoje não mapeado |
| `liberacoes_acumuladas`, `valor_da_operacao` | 0,9% | desprezível |

`entrada_bb` **não tem** coluna de investimento total nem de contrapartida.
Antes da change o único valor que chegava ao silver era o VGV, sob o rótulo de
empréstimo; agora vai em `valor_vgv` e `valor_financiamento` fica nulo.

### `bext` (47 colunas)

| coluna física | fill | conceito canônico |
|---|---:|---|
| `mvalor_investimento` | ~100% | `valor_investimento_total` |
| `mvalor_emprestimo` | ~100% | `valor_financiamento` |
| `mvalor_financiamento` | ~100% | `valor_financiamento` (par de `mvalor_emprestimo`) |
| `mvalor_desembolso` | ~100% | `valor_desembolsado_acumulado` |
| `mvalor_subsidio` | ~100% | `subsidio_total` (sem split FGTS/OGU) |
| `mvalor_contrapartida_poder_publico` | ~100% | **`valor_contrapartidas`** (hoje **descartada**) |
| `valor_do_emprestimo` | <0,1% | ruído de uma geração — não usar |

## 3. GEFUS / INT (empreendimento, SFTP) — desembolso e componentes

| interface | frente | total investido | desembolso acumulado | componentes de liberação |
|---|---|---|---|---|
| INT040 | FAR CAIXA | `vr_investimento` | `vr_liberado` | `liberacao_obra`, `liberacao_terreno`, `liberacao_ts`, `liberacao_ts_adicional` + `vr_contrapartida_1` |
| INT054 | FAR BB | `vr_investimento` | `total_liberado_far` | `a_liberar_obra`, `a_liberar_terreno`, `a_liberar_ts`, `liberacao_ts_adicional` + `vr_contrapartida_1` |
| INT059 | FDS | `vr_investimento` | `vr_liberado` / `vr_liberado_sisfin` | `vr_projeto`, `vr_obra`, `contrapartida_financeira`, `contrapartida_servicos`, 6× `contrapartida_poder_pub_local_*` |
| INT057 | Rural BB | `vr_investimento` | `vr_liberado` | `vr_edificacao`, `vr_atec`, `vr_ts`, `vr_custo_originacao`, … + `vr_contrapartida` |
| INT065 | Rural CAIXA | `vr_investimento_pnhr` | `vr_liberado` | `vr_edificacao`, `vr_atec`, `vr_ts`, … + `vr_emprestimo_siapf`, `vr_subsidio_fgts`, `vr_contrapartida` |

Empréstimo: `vr_emprestimo_far` (INT054), `vr_emprestimo_original` (INT059),
`vr_emprestimo` / `vr_emprestimo_siapf` (INT065). Hoje **não** propagados às
silvers por frente (as silvers só carregam `valor_contratado` e
`valor_desembolsado`).

SNH (`bronze_mcmv_historico_empreendimento_snh_bb/_caixa`):
`valor_contratado`, `valor_desembolsado`, `valor_desembolsado_do_ano_de_referencia`,
`valor_aporte_adicional`.

## 4. `*_financeiro_mensal` (SharePoint) — decomposição por componente

**São snapshot único**, não série: `dt_referencia` constante, `dt_liberacao`
desde 2024-06. Não é fonte do desembolso total do acompanhamento histórico —
ver D4 do design.

| bronze | total | componentes |
|---|---|---|
| `bronze_far_financeiro_mensal` | `vr_liberado`, `vr_movimento` | `vr_pago_obra`, `vr_pago_terreno`, `vr_pago_trabalho_social`, `vr_pago_equipamentos`, `vr_pago_aporte`, `vr_pago_manutencao`, `vr_pago_incc`, `vr_pago_legalizacao` |
| `bronze_fds_financeiro_mensal` | `vr_liberado`, `vr_movimento` | `vr_pago_obra`, `vr_pago_terreno`, `vr_pago_trabalho_social`, `vr_pago_projeto`, `vr_pago_aporte`, `vr_pago_incc`, `vr_pago_legalizacao`, `vr_pago_seguranca` |
| `bronze_rural_financeiro_mensal` | `vr_movimento` (sem `vr_liberado`) | `vr_desembolso_obra`, `vr_desembolso_trabalho_social`, `vr_desembolso_atec`, `vr_desembolso_cisternas_efluentes`, `vr_desembolso_custos_indiretos` |

FAR: `Σ(vr_pago_*) = vr_liberado` fecha exato. FDS: `Σ(vr_pago_*) ≈ 19%` de
`vr_liberado` — investigar (tarefa 7.1). Rural: sem total, monta bottom-up.

## 5. Achados que viram teste ou nota (não corrigidos aqui)

- **`entrada_bb` VGV no rótulo de empréstimo**: 246,5 bi somados (nominais, com
  multiplicação de snapshot); média R$ 27 M/linha vs R$ 7,7 M em
  `bases_relatorio`. → tarefa 4.1.
- **`bext` contrapartida descartada**: `mvalor_contrapartida_poder_publico`
  100% preenchida, nunca chega ao silver. → tarefa 4.2.
- **Negativos** (`serie_executiva`, silver materializado):
  `bases_relatorio` 1.312 `valor_investimento < 0`, 960 `subsidio_ogu < 0`
  (mín. −R$ 122,8 M, chave `38433765`/SP repetida ~33 meses);
  `bext` 24.655 `valor_investimento < 0`, 20.848 `subsidio_total < 0`.
- **Outlier** `bext`/`2694194`/SP: R$ 2,40 bi de investimento para 128 UH,
  congelado por 12 meses (2017-03 → 2018-08). → seed de quarentena.
- **R$/UH `valor_investimento / uh_contratadas`** (linhas com ambos > 0):
  `bases_relatorio` p50 R$ 53 k, p99 R$ 325 k, máx R$ 23,9 M;
  `bext` p50 R$ 28 k, p99 R$ 136 k, máx R$ 25,0 M;
  `min_cidades` p50 R$ 74 k, p99 R$ 146 k, máx R$ 184 k.
- **`min_cidades` / `bext` sem `uf`** nas gerações antigas → `regiao_*` nula.
- Valores **nominais** 2012–2018 — sem deflação (D8).
