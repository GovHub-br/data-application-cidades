# dedup-fonte-silver-historico — reconciliação antes/depois

Change `dedup-fonte-silver-historico`. Materialização **só local** (`--target
staging_duckdb`, `cidades.duckdb`) — nada escrito no MinIO.

## O problema

As 3 `silver_*_historico_empreendimento` declaram grão **empreendimento × mês**
mas entregavam **empreendimento × `dt_referencia`**, e as duas fontes gravam
`dt_referencia` em dias diferentes:

| braço | `dt_referencia` na origem |
|---|---|
| SFTP GEFUS (`bronze_gefus`: INT040/054/057/059/065) | **fim do mês** (`_YYYYMMDD.parquet` → 25–31) |
| SNH (`bronze_snh_empreendimento`) | **dia 1** (`%Y%m` → `::date`) |

Como `2024-06-30 ≠ 2024-06-01`, o `dedup` por `(frente_mcmv, apf, dt_referencia)`
**nunca juntava** as duas linhas na janela de sobreposição → ambas sobreviviam.
O `unique` de `id_historico_snapshot` passava (o hash inclui `dt_referencia::text`)
— falsa confiança.

Segundo efeito: quando a linha SNH vence a dedup, `valor_contratado` /
`valor_desembolsado` / `responsavel_*` (que só o SFTP preenche) somem e não havia
carry-forward para elas → `gold_snapshot` FAR com `responsavel_nome` em 15%.

## O que mudou

1. **D1** — `dt_referencia` normalizado por `date_trunc('month', ·)::date` na
   projeção de **cada braço** (SFTP e SNH), antes do `enriquecido`/`dedup`. Dia
   exato do corte fica em `dt_movimento` (100% preenchido nos dois braços).
2. **D2** — grão de dedup `(frente_mcmv, apf, dt_referencia)` agora colapsa
   SFTP+SNH do mesmo APF/mês; precedência SNH inalterada. `id_historico_snapshot`
   hasheia o mês (fórmula intacta). Teste `unique_combinacao` `(frente_mcmv, apf,
   dt_referencia)` `error` nas 3 silvers.
3. **D3** — LOCF de coluna em `macros/historico/silver_tail.sql` (CTE
   `preenchido`, após a dedup e o carry-forward de linha): `valor_contratado`,
   `valor_desembolsado`, `responsavel_id`, `responsavel_nome` preenchidos com a
   última observação não-nula do mesmo `(frente_mcmv, apf)`. Marcadores
   `valor_contratado_preenchido` / `responsavel_preenchido` (BOOLEAN, novos no
   contrato comum + `gold_snapshot`). `percentual_execucao_fisica` /
   `status_operacional` / `situacao_canonica` **não** são preenchidos.
4. **D4** — `ouro_dhist_serie_situacao_mensal`: removido `case fonte_serie when 'snh'`
   do `qualify` do CTE `silvers` (a silver já entrega o grão). Mantido o
   desempate de fase para FDS multi-fase.

### Decisão do marcador de LOCF (Open Question 1)

Marcador **booleano por grupo**, não extensão de `fonte_valor`:
`valor_contratado_preenchido` (cobre valor_contratado + valor_desembolsado) e
`responsavel_preenchido`. `fonte_valor` continua `observado`/`carregado` e
descreve a **linha inteira** (carry-forward de linha); o LOCF de coluna é
ortogonal — a linha é uma observação SNH real, só o valor foi herdado.

### `percentual_execucao_financeira` pós-LOCF (Open Question 2)

**Não recalculado.** O `%` derivado (`valor_desembolsado / valor_contratado`) é
calculado no corpo da silver, antes da cauda/LOCF. Quando o insumo estava
ausente e depois foi LOCF-preenchido, o `%` **permanece NULL** (não-derivável) —
mistura de desembolso observado com contratado carregado (ou vice-versa) daria
razão enganosa. O marcador `valor_contratado_preenchido` expõe o caso para quem
quiser recalcular a jusante.

### `source_file` como desempate estável (Open Question 3)

Mantido `order by ..., source_file` no `dedup`. Verificado: **0** grupos
`(frente, apf, mês)` com 2+ linhas SNH nas 3 silvers (a SNH já grava dia 1 → já
está no grão mensal). `prioridade_reentrega` do bronze SNH **não** foi trazido
ao contrato — desnecessário hoje; reavaliar se a SNH passar a ter reentregas
no mesmo mês.

## Linha de base (ANTES) — build local `cidades.duckdb` 2026-09-07

### Hash do conteúdo ordenado (colunas de timestamp excluídas)

| gold | hash (ANTES) | hash (DEPOIS) |
|---|---|---|
| `ouro_dhist_serie_situacao_mensal` | `11da99d94cbe0926900b68c703a26d68` | _(preencher no 5.2 — DEVE bater)_ |
| `ouro_dhist_snapshot_empreendimento_atual` | `2b72d494f6079cdc23601ff4f97c4978` | _(muda só onde o LOCF recupera valor/responsável)_ |
| `ouro_dhist_marco_empreendimento` | `da10181f180c78e1fef0492e9494e6ba` | _(preencher)_ |

### Silvers por frente — `count(*)`, fill, duplicatas de grão mensal

| frente | `count(*)` | fill valor_contratado | fill valor_desembolsado | fill responsavel_nome | linhas `carregado` | grupos duplicados `(frente,apf,mês)` |
|---|---|---|---|---|---|---|
| FAR   | 343 813 | 92.0% | 89.5% | 72.8% | 836   | **15 228** |
| FDS   |  53 517 | 91.2% | 88.8% | 73.4% | 75    | **8 535** |
| Rural | 740 801 | 96.1% | 90.7% | 72.1% | 4 356 | **44 566** |

### `ouro_dhist_snapshot_empreendimento_atual` — fill por frente (ANTES)

| frente | `count(*)` | fill valor_contratado | fill valor_desembolsado | fill responsavel_nome |
|---|---|---|---|---|
| Entidades |    917 | 98.6% | 97.9% | 91.1% |
| FAR       |  5 506 | 73.6% | 60.8% | **15.5%** |
| Rural     | 11 122 | 87.1% | 63.6% | **3.7%** |

`ouro_dhist_marco_empreendimento`: Entidades 917 · FAR 5 506 · Rural 11 122.

### Amostra 1.3 — APF FAR com SFTP+SNH no mesmo mês (jun–nov/2024)

`16517361`, `18283735`, `19095100` (meses 2024-06..11, 2 linhas cada:
`sftp,snh`). Pós-fix devem ter 1 linha/mês, `fonte_serie = 'snh'`,
`dt_referencia` no dia 1, `valor_contratado`/`responsavel_nome` preenchidos por
LOCF do SFTP.

## Resultado (DEPOIS) — build direcionado 2026-09-07

`dbt run --target staging_duckdb --select` das 3 silvers + 3 golds (~14 s, sem
estouro de RAM). `dbt test` das 3 silvers + 3 golds: **PASS=193 WARN=18
ERROR=0**. Os 3 `unique_combinacao (frente_mcmv, apf, dt_referencia)` = **PASS**.
Nenhum ERROR novo (o `desembolso_nao_excede_contratado` `error` do Rural segue
verde — a quarentena por APF cobre).

### Hash dos golds

| gold | ANTES | DEPOIS | veredito |
|---|---|---|---|
| `ouro_dhist_serie_situacao_mensal` | `11da99d94cbe0926900b68c703a26d68` | `11da99d94cbe0926900b68c703a26d68` | **byte-idêntico ✓** |
| `ouro_dhist_snapshot_empreendimento_atual` | `2b72d494…` | muda (LOCF + dia do mês) | ver abaixo |
| `ouro_dhist_marco_empreendimento` | `da10181f…` | muda só em `*_dt_snapshot` / `*_fonte` | ver abaixo |

**Nota D4 — o `case fonte_serie when 'snh'` FICOU no `qualify` de
`ouro_dhist_serie_situacao_mensal`.** Removê-lo (como o texto literal da task 5.1 pedia)
**quebrou o byte-idêntico**: 453 linhas do FDS mudaram porque, num empreendimento
FDS multi-fase, o termo escolhe qual **APF-fase** representa o empreendimento no
mês — o APF cuja observação do mês vem do SNH prevalece sobre o APF que só o SFTP
reportou. Sem ele, a fase Obra de um APF SFTP passava a ganhar da fase Projeto do
APF SNH → `nao_iniciada` caía (~39→~26/mês), `em_obras` subia. Isso **não** é
re-dedup de fonte (a silver já resolve fonte dentro de um APF); é seleção
entre APFs, que a silver não faz. O termo foi mantido com comentário honesto.
A re-dedup SFTP×SNH **por APF** saiu de fato (`dt_referencia desc` deixou de
depender do dia; FAR/Rural viraram no-op).

### Silvers por frente — `count(*)`, fill, duplicatas de grão mensal

| frente | `count(*)` antes→depois | fill valor_contratado | fill valor_desembolsado | fill responsavel_nome | grupos duplicados `(frente,apf,mês)` |
|---|---|---|---|---|---|
| FAR   | 343 813 → **328 585** (−15 228) | 92.0 → **99.4%** | 89.5 → **99.5%** | 72.8 → **95.8%** | 15 228 → **0** |
| FDS   |  53 517 → **44 982** (−8 535)  | 91.2 → **96.3%** | 88.8 → **99.9%** | 73.4 → **95.9%** | 8 535 → **0** |
| Rural | 740 801 → **696 235** (−44 566) | 96.1 → **100.0%** | 90.7 → **98.7%** | 72.1 → **97.9%** | 44 566 → **0** |

Efeito colateral positivo: `acumulado_nao_regride` de `valor_desembolsado` cai
(FAR 10 473→2 769 quedas; Rural 43 770→16 994) — o zigue-zague SFTP(fim de
mês)×SNH(dia 1) dentro do mês desapareceu. `quantidade_uh_concluidas` sobe um
pouco (FAR 3 509→6 997) — a coluna, antes NULL nas linhas SNH da janela
sobreposta, agora é preservada do SFTP no grão mensal e expõe o ruído mensal
pré-existente do `qt_unidades_concluidas` do GEFUS. Tudo `warn`.

### `ouro_dhist_snapshot_empreendimento_atual` — fill por frente

| frente | `count(*)` | fill valor_contratado | fill responsavel_nome |
|---|---|---|---|
| Entidades |    917 (=) | 98.6 → **99.8%** | 91.1 → **91.2%** |
| FAR       |  5 506 (=) | 73.6 → **97.9%** | **15.5 → 84.7%** |
| Rural     | 11 122 (=) | 87.1 → **99.8%** | **3.7 → 86.3%** |

Contagem de empreendimentos por frente **inalterada**. Mudou: `responsavel_id/nome`
(~13 k linhas, LOCF), `valor_contratado` (2 757), `valor_desembolsado` (5 808),
e `id_historico_snapshot` / `dt_referencia` / `dt_snapshot_efetivo` (2 105 —
último snapshot era SFTP, `dt_referencia` foi de fim-de-mês p/ dia 1).

### `ouro_dhist_marco_empreendimento`

Row count por frente inalterado. **Nenhum valor de marco (`dt_*`) mudou**;
`marcos_coerentes` inalterado. Mudaram só as colunas de proveniência:
`*_dt_snapshot` (dia do mês 30→1, mesmo mês — consequência do D1) e
`dt_ultima_entrega_fonte` (5 850) / `dt_conclusao_obra_fonte` (85), que passam a
apontar `sftp:INTxxx` onde a silver agora preserva o valor do SFTP no grão
mensal (rótulo mais fiel — o valor sempre foi o do SFTP).

### Amostra 1.3 — APF FAR (16517361, 18283735, 19095100), jun–nov/2024

Pós-fix: 1 linha por mês, `fonte_serie = 'snh'`, `dt_referencia` no dia 1,
`valor_contratado` / `responsavel_nome` preenchidos (LOCF do SFTP até 2024-11).

### Materialização

Só `cidades.duckdb` (`--target staging_duckdb`). Nenhuma execução de
`publicar-historico.sh`, nenhum upload ao MinIO.

## Open Questions residuais

- **OQ4** — `gold_snapshot` do FDS quando o SFTP INT059 parar (hoje vai a
  2026-06): cai no mesmo cenário do FAR; o LOCF já cobre. Nenhuma data de corte
  hard-coded encontrada.
