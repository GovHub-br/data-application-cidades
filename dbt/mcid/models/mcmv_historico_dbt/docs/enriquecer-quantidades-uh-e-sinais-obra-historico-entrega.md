# Entrega — enriquecer-quantidades-uh-e-sinais-obra-historico

Change OpenSpec `enriquecer-quantidades-uh-e-sinais-obra-historico`. Build local
(`--target staging_duckdb`, `/mnt/data/duckdb/cidades.duckdb` + MinIO
`10.0.0.56:9000/data-lake-mcid`). Nada promovido a prod / carga noturna.

Este doc cobre o levantamento (fase 1) e a entrega. Serve tanto como
`docs/enriquecer-quantidades-uh-entrega.md` (task 1.1) quanto como o doc de
entrega da task 8.4 (`models/docs/entregas/...`) — arquivo único.

---

## 1. Levantamento (2026-09-06, `cidades.duckdb` local + MinIO real)

### 1.1 Colunas de UH nas bronzes — preenchimento medido

| Coluna fonte | Bronze | Preench. real | Observação |
|---|---|---|---|
| `quantidade_de_uhs_distratadas` | `snh_bb` | 21.896 / 21.896 (100%) | ~0 na maioria; distratos > 0 ≈ centenas de linhas. `0` é informação. |
| `quantidade_de_uhs_distratadas` | `snh_caixa` | 250.523 / 287.401 (87%) | idem — 249.938 linhas são `0`, ~600 linhas com valor real. |
| `uh_vigentes` | `snh_bb` | 14.168 / 21.896 (65%) | BB usa `uh_vigentes` **e** `uhs_vigentes` (35%) — coalesce das duas. |
| `uh_vigentes` | `snh_caixa` | 287.401 / 287.401 (100%) | — |
| `qt_unidades_ociosas` | `int040` | 239.021 / 239.022 (100%) | inclui sentinela `-1` (322 linhas) → mapear p/ NULL. Mais de 180 k são `0`. |
| `qt_unidades_ociosas` | `int054` | 11.338 / 11.339 (100%) | idem. |
| `qt_unidades_ociosas` | `int057` / `int059` / `int065` | — | **coluna não existe** → NULL nos braços Rural BB / FDS. |
| `qtde_uh_inicial` | `int065` | 470.252 / 473.976 (99%) | diverge de `qtde_unidades` em **33.868** linhas (~7%); em 33.573 delas `inicial > atual` (corte de escopo). |
| `pc_execucao_financeira_obra` | `int057` | 61.993 / 61.993 (100%) | pt-BR (`83,82`). |

### 1.2 `cod_pendencia_*` — DIVERGÊNCIA vs. proposal (Open Question 5 fechada)

O proposal afirmava `cod_pendencia_entrega` **69% / 164 k linhas** — "a única
variável explicativa de atraso que o GEFUS já entrega". **Não se sustenta contra
a bronze materializada:**

| Coluna | `int040` valores | `int054` valores |
|---|---|---|
| `cod_pendencia_entrega` | `''` (221.866) · `'NULL'` (17.156) | `''` (11.138) · `'NULL'` (201) |
| `cod_pendencia_obra` | `''` (220.285) · `'NULL'` (17.032) · `'NAO'` (1.485) · `'SIM'` (220) | `''` (11.138) · `'NULL'` (201) |

`cod_pendencia_entrega` tem **zero** valor real. `cod_pendencia_obra` só carrega
`SIM`/`NAO` em ~1.7 k linhas do INT040.

**Decisão (usuário, 2026-09-06):** carregar **só `cod_pendencia_obra`** como
texto cru (braços INT040/INT054; NULL nos demais). **`cod_pendencia_entrega`
sai de escopo.** Spec e proposal atualizados.

### 1.3 Schema real de `MONIT_MOV_OBRA_*` — as 3 frentes DIVERGEM

Inventário sob `staging/sharepoint/Novo MCMV - */**` (seleção pelo nome do
arquivo, `MONIT_MOV_OBRA_<FRENTE>_MENSAL_`, não pela pasta):

| Frente | `_MENSAL_YYYYMM` (não-LAYOUT) | Janela | linhas/mês |
|---|---|---|---|
| FAR | 202512, 202601-202605, 202607 (falta 202606) | 7 arquivos | ~820-880 |
| FDS | 202512, 202601-202607 | 8 arquivos | ~300 |
| RURAL | 202512, 202601-202607 | 8 arquivos | — |

- **Não há `_MENSAL` anterior a 202512.** Os semanais de 2025-04+ (FAR) estão
  em `Arquivados/` mas ficam de fora (só `_MENSAL` na primeira entrega — Open
  Question 1 fechada: cadência mensal apenas).
- `_LAYOUT_` = arquivos `_YYYYMMDD` de dicionário de campos (colunas `ordem`,
  `campo`, `descricao`) — não são dados. Excluídos pelo filtro `_MENSAL`.
- Arquivos FDS/RURAL de 202602+ estão fisicamente sob `Novo MCMV - FAR/`
  (misfiled) — capturados pelo filtro no nome.

**Mapa de colunas fonte → contrato da silver de obra** (FAR ≠ FDS ≠ RURAL):

| Contrato silver | FAR | FDS | RURAL |
|---|---|---|---|
| `dt_movimento` | `dt_movimento` | `dh_movimento` | `dh_movimento` |
| `co_situacao_obra` | `co_situacao_obra` | `co_situacao_operacao` | `co_situacao_operacao` |
| `co_andamento_operacao` | — | `co_andamento_operacao` | `co_andamento_operacao` |
| `dt_alteracao_situacao` | `dt_alteracao_situacao` | `dt_alteracao_situacao` | `dt_alteracao_situacao` |
| `pc_obra_prevista` / `_realizada` | ✓ | ✓ | ✓ |
| `co_classificacao_paralisado` | `co_classificacao_paralisados` | idem | idem |
| `dt_paralisacao` | ✓ (0%) | ✓ (0%) | ✓ (0%) |
| `detalhe_paralisacao` | — | `detalhe_paralisacao_retomada` | `no_detalhe_paralisacao_retomada` |
| `dt_previsao_conclusao_obra_retomada` | `dt_previsao_conclusao_obra_retomada` | `dt_prev_conclusao_obra_retomada` | `dt_previsao_conclusao_obra_retomada` |
| `dt_conclusao_obra_retomada` | ✓ | ✓ | ✓ |
| `qt_uh_alienadas` | `qt_uh_alienada` | `qt_uh_alienada` | `qt_uh_alienada` |
| `qt_uh_sem_habitese` | ✓ | ✓ | ✓ |
| `qt_uh_construcao_parcial` | `qt_uh_em_construcao_parcial` | idem | idem |
| `qt_uh_ociosas_retomadas` | `qt_uh_ociosas_retomadas` | idem | idem |
| `qt_unidades_habitacionais_invadidas` | `qt_unidades_habitacionais_invadidas` | `qt_uh_ocupacao_irregular` | `qt_uh_ocupacao_irregular` |
| `qt_uh_concluidas` | `qt_uh_concluidas` | `qt_uh_concluidas` | `qt_uh_concluidas` |
| `dt_conclusao_obra` | `dt_conclusao_obra` | `dt_conclusao_obra` | `dt_conclusao_obra` |
| `dt_legalizacao` | `dt_legalizacao` | `dt_legalizacao_reg` | `dt_legalizacao_reg` |
| `dt_previsao_entrega` | `dt_previsao_entrega_do_empreendimento` | `dt_prev_entrega_emprend` | `dt_previsao_entrega_do_empreendimento` |
| `dt_entrega` | `dt_entrega_do_empreendimento` | idem | idem |
| `dt_acion_seguradora` (só FAR) | `dt_acion_seguradora` | — | — |
| `dt_contrata_construtor_substituto` (só FAR) | ✓ | — | — |
| `dt_repactuacao` (só FAR) | `dt_repactuacao` | — | — |

`coalesce_present` sobre a relação de cada bronze absorve a coluna ausente
(compila como `NULL::<tipo>`).

**Realidade dura:** `dt_paralisacao` 0% em todos os arquivos.
`co_classificacao_paralisados` 0% no FAR, 100% no FDS (código, majoritariamente
"não paralisado"). `pc_obra_prevista` e `co_situacao_obra`/`_operacao` 100%
preenchidos — são o sinal central. `co_situacao_obra` domínio observado:
`1,2,3,5,11,16` (a decodificar em follow-up — Non-Goal 2).

### 1.4 Coalesce SNH×GEFUS vs. colegas (D5 / Open Question 3)

As silvers históricas por frente **fazem coalesce** (SNH vence 2024-06→2024-11);
os colegas (`silver_fds_empreendimento` etc.) mantêm `snh_*` paralelas +
`tem_dados_snh`. Esta change **não muda** o coalesce (BREAKING, fora de escopo).
Direção proposta para a harmonização futura (change à parte): expor colunas
`snh_uh_*` paralelas + `fonte_valor_uh` por coluna, em vez do coalesce
destrutivo. Registrado aqui como decisão consciente; nada feito nesta entrega.

---

## 2. Entrega

Escopo de materialização: **só local** (`--target staging_duckdb`,
`run-historico.sh` / `run-reloginho.sh`). Nada promovido a `prod` nem à carga
noturna.

### 2.1 Contrato comum das silvers por frente — 10 colunas novas

Ao fim do contrato (antes de `dt_silver`), `NULL` onde a fonte do braço não
reporta. Propagadas em `ouro_dhist_snapshot_empreendimento_atual` (posições 39-48;
prefixo 1-38 inalterado).

| coluna | fonte | tipo | fill (build local) |
|---|---|---|---|
| `quantidade_uh_distratadas` | braço SNH (`quantidade_de_uhs_distratadas`) | bigint | FAR 82.384 (>0: 535) · FDS 12.571 (>0: 0) · Rural 181.043 (>0: 114) |
| `quantidade_uh_vigentes` | braço SNH (`uh_vigentes` ∪ `uhs_vigentes`) | bigint | FAR 93.559 · FDS 14.221 · Rural 205.202 |
| `quantidade_uh_ociosas` | SFTP INT040/INT054 (`qt_unidades_ociosas`; `-1`→NULL) | bigint | FAR 249.932 · FDS 0 · Rural 0 |
| `quantidade_uh_inicial` | SFTP INT065 (`qtde_uh_inicial`) | bigint | FAR 0 · FDS 0 · Rural 469.867 |
| `cod_pendencia_obra` | SFTP INT040/INT054, texto cru | text | FAR 1.705 (`NAO` 1.485 / `SIM` 220) · FDS 0 · Rural 0 |
| `percentual_execucao_financeira` | `reportada` (INT057) ∪ `derivada` (`desembolsado/contratado*100`) | double | FAR 303.265 (der) · FDS 45.726 (der) · Rural 674.656 (rep 61.936 / der 612.720) |
| `percentual_execucao_financeira_fonte` | — | text | `reportada` / `derivada` / NULL |
| `gap_fisico_financeiro_pp` | `pef − percentual_execucao_fisica` (negativo preservado) | double | FAR 303.259 · FDS 45.709 · Rural 670.711 |
| `fonte_valor` | `observado` / `carregado` (carry-forward) | text | carregado: FAR 836 · FDS 75 · Rural 4.356 |
| `dt_snapshot_efetivo` | mês da observação real | date | 100% |

**Reloginho** (`prata_dhist_snh_apf_mes`): `quantidade_uh_distratadas`
adicionada (após `uh_vigentes`). Grão `(agente_financeiro, apf, dt_referencia)`
intacto. Golds `ouro_reloginho_indicadores` / `_frente` / `_entregas` fazem
agregação com projeção explícita — **não referenciam a coluna nova**, saída
inalterada.

### 2.2 Carry-forward do snapshot SNH intermitente (D6)

Tail comum `macros/historico/silver_tail.sql` (`historico_silver_tail()`).
Janela SNH por `(frente, apf)`; meses faltantes recebem a última observação até
`var('carry_forward_max_meses', 3)`; `dt_movimento` = NULL na linha carregada
(mantém `assert_empreendimentos_dt_movimento_consistente`). **Não interpola.**

Serrote eliminado — FAR `sum(quantidade_uh)` nacional: 2025-02
`1.322.472 → 1.470.066`, 2025-11 `1.346.800 → 1.494.394`. Em
`ouro_dhist_serie_situacao_mensal` as linhas carregadas repetem `situacao_canonica`
→ 0 transições falsas → só entram no estoque. **Decisão: não filtrar
`fonte_valor='observado'`** nesse gold.

### 2.3 Família bronze `obra_mensal` + o modelo de obra mensal autônomo

- Mapa `familias_obra_mensal()` (3 frentes), corpo `bronze_obra_mensal`, 3
  cascas finas. Glob `sharepoint/Novo MCMV - */**/*MONIT_MOV_OBRA_<FRENTE>_MENSAL_*.parquet`
  (frente no nome; misfiles sob `Novo MCMV - FAR/` capturados).
- **Janela real 202512 → 202607** (FAR sem 202606). Não há obra mensal antes de
  202512 — os semanais de 2025-04+ ficaram de fora (OQ1).
- Bronze: FAR 5.848 / FDS 2.783 / Rural 12.107 linhas.
- Modelo de obra mensal autônomo (grão
  `frente_mcmv × apf × dt_referencia`, 20.738 linhas / 2.757 APF, 0 dupes).
  Schemas divergentes das 3 frentes harmonizados por `coalesce_present` com
  lista de aliases (`macros/historico/obra_mensal_arm.sql`).
- **Fill medido**: `pc_obra_prevista` / `pc_obra_realizada` ~100% (o sinal
  central); `co_situacao_obra` 100% (código cru {1,2,3,4,5,11,16}, a decodificar
  — Non-Goal 2); `qt_uh_alienadas` 12.132 (Rural ~100%, FAR 0%);
  `co_classificacao_paralisado` só FDS; `dt_paralisacao` **0%** (coluna existe,
  vazia); `qt_uh_concluidas`, `detalhe_paralisacao`, `dt_acion_seguradora`,
  `dt_repactuacao` **0%** nesse feed.
- **D4**: NÃO entra no `left join` do contrato comum das silvers por frente —
  modelo paralelo. Costura = follow-up (OQ2).

### 2.4 Macros DQ novas (`warn`, só listam)

- `acumulado_nao_regride(column, partition_by, order_by)` — quedas mês-a-mês em
  acumulados.
- `quantidade_nao_excede_referencia(column, reference, fator=1.0)` — espelha
  `desembolso_nao_excede_contratado`.
- `reconcilia_decomposicao` reusada para `vigentes ≈ contratadas − distratadas`
  (spec `quantidades-uh-historico`).

Contagens `dbt test` (as 3 silvers): PASS=76 WARN=18 ERROR=0.

| teste | FAR | FDS | Rural |
|---|---|---|---|
| `acumulado_nao_regride` entregues | **13.058** | 909 | 345 |
| `acumulado_nao_regride` concluidas | 3.509 | 70 | 47 |
| `acumulado_nao_regride` desembolsado | 10.473 | 1.588 | 43.770 |
| `quantidade_nao_excede_referencia` entregues | **1.036** | 4 | 0 |
| `quantidade_nao_excede_referencia` concluidas | 1.348 | 0 | 0 |
| `reconcilia_decomposicao` vigentes×distratadas | 708 | 130 | 106 |

Nenhum altera a materialização (contagem de linhas idêntica com/sem os testes).

### 2.5 Open Questions em aberto (não bloqueiam a entrega)

| # | assunto | estado |
|---|---|---|
| 1 | cadência da `obra_mensal` (mensal × semanal) | **fechada** — só `_MENSAL` |
| 2 | costurar `pc_obra_prevista` / `co_situacao_obra` no contrato comum de 2019+ | **aberta** — follow-up; silver de obra fica paralela (D4) |
| 3 | harmonizar coalesce SNH×GEFUS com os colegas (`snh_*` paralelas) | **aberta** — direção registrada (§1.4); change à parte |
| 4 | fonte GEAVO (`situacaodaobra` FGTS/setor público) | **aberta** — cruzamento por APF a validar |
| 5 | decodificar `cod_pendencia_*` | **fechada** — `_entrega` vazio → fora; `_obra` entra como texto cru, decodificação é follow-up |

Follow-ups: decodificar `co_situacao_obra` / `co_classificacao_paralisado` /
`cod_pendencia_obra` em seed; costura da silver de obra ao contrato de 2019+
(OQ2); harmonização SNH×GEFUS (OQ3).
