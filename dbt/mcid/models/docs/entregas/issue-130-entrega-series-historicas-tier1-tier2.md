# Issue #130 — Entrega: séries históricas Tier 1 (entregas por evento) e Tier 2 (série executiva pré-2024)

> Data: 2026-09-02. Branch `feat/tratamento-dados-historicos`.
> Substitui o rascunho `issue-130-proposta-bronze-series-historicas.md` (agora implementado).
> Motivação: um dos objetivos do projeto é usar os dados históricos para **análise
> preditiva** (tendência, sazonalidade, drift, backtest do relógio) — o que exige
> série mensal longa de contratação/entrega, que a SNH (2024-06+) não cobre.
> Pré-requisito atendido: re-tratamento do `staging/dados_historicos` concluído
> (390 parquets canônicos; log em `data/minio_conversao_parquet_log.csv`).

---

## Tier 1 — Entregas por evento (SNH)

Complementa `bronze_reloginho_snh_serie_mensal` (acumulado) com o **fluxo** real de
entregas: quando cada UH foi entregue, não só o total acumulado no mês do snapshot.

| Modelo | Papel |
|---|---|
| `indicadores_mcmv_dbt/bronze/bronze_reloginho_snh_entregas_evento.sql` | Cópia fiel de `o_recente_*_af_caixa_entregas` (CAIXA) + `*_da_entrega_da_unidade_af_bb` (BB). union_by_name; `dt_evento` = coalesce(dt_entrega, dt_ass_doc); `qt_uh_entregues_evento`; `hash_linha`. |
| `indicadores_mcmv_dbt/silver/silver_reloginho_snh_entregas_mes.sql` | Dedup de eventos por `hash_linha`; soma por `(agente, apf, mês do EVENTO)`. Grão = fluxo mensal de entregas por APF. |
| `indicadores_mcmv_dbt/gold/indicadores_reloginho_entregas.sql` | Fluxo por evento (mês e acumulado) **vs** acumulado do snapshot (`indicadores_reloginho`), lado a lado. `dif_evento_vs_snapshot` deve tender a ~0. |

**Ganhos:**
- Resolve a decisão #5 da #130 (qual é o total oficial de entregas: 1.518.598 por
  evento vs 1.543.432 pelo acumulado) — os dois caminhos ficam materializados e
  comparados.
- Habilita o `ritmo_recente` (média móvel de entregas por evento) para
  `projecao_entrega`.
- A data do evento vai muito além de 2024-06: os snapshots SNH reportam entregas de
  empreendimentos antigos → série de entregas efetivas desde ~2010 (a confirmar no
  volume real).

---

## Tier 2 — Série executiva histórica (~2010-2018)

| Modelo (`models/mcmv_historico_dbt/serie_executiva/`) | Papel |
|---|---|
| `bronze_mcmv_serie_executiva_historica.sql` | Cópia fiel: `UNION ALL BY NAME` de 4 famílias de `staging/dados_historicos/` — `bases_relatório_executivo` (61), `min_cidades` (71), `entrada_bb` (50), `bext` (21). `fonte_familia`, `dt_referencia` (mês-snapshot via `hist_dt_referencia`), `hash_linha` surrogate. |
| `silver_mcmv_serie_executiva_historica.sql` | Contrato comum via `coalesce_present()` (mapa de colunas abaixo); tipagem (`parse_hist_*`); `linha_ogu_fgts` (OGU/Subsidiado vs FGTS/Financiado); dedup por `(fonte_familia, chave_natural, dt_referencia)` mantendo o snapshot mais recente do mês. Grão = registro de origem. |
| `gold_mcmv_serie_historica_mensal.sql` | Agrega por `(dt_referencia, fonte_familia, nivel_agregacao, uf, linha_ogu_fgts)` via `GROUPING SETS` (nacional + uf). `prioridade_familia` para escolher **uma** série contínua (não somar entre famílias — se sobrepõem 2014-2016). |

### Mapa de colunas → contrato comum (`coalesce_present`)

| Campo comum | Aliases de origem (por geração de schema) |
|---|---|
| `chave_natural` | `cod_apf` · `codapf` · `cod_empreendimento` · `icodigo_empreendimento` · `codigo_empreendimento_bb` · `cod_contrato` · `nr_prpt` · `contrato_bb` · `contrato_caixa` |
| `uf` | `uf` · `csigla_uf` |
| `codigo_ibge_municipio` | `cod_munic_ibge` · `codmunicibge` · `cod_municipio` · `codigo_do_ibge` · `icodigo_municipio_ibge_sem_dv` |
| `faixa` | `faixa` · `cfaixa` · `num_faixa` · `faixa_divisao` |
| `uh_contratadas` | `uh` · `unidades` · `iqde_uh` · `qtd_unidade_habitacional` · `qtd_uh` · `qde_unidades` |
| `uh_entregues` | `iqde_unidades_entregues` · `unidades_entregues` · `iqde_uh_entregues` · `qtd_unidade_entregue` · `qtd_entregue` · `entregues` |
| `uh_concluidas` | `uh_concluidas` · `unidades_concluidas` · `iqde_uh_concluidas` · `qtd_unidade_concluida` · `qtd_concluida` · `uh_concluidos` |
| `uh_em_obras` | `uh_em_obras` · `unidades_em_obras` · `iqde_uh_em_obras` |
| `valor_investimento` | `valor_total_do_investimento` · `mvalor_investimento` · `vlr_total_operacao` · `vlr_total_investimento` |
| `valor_emprestimo` | `valor_do_emprestimo` · `mvalor_emprestimo` · `vlr_emprestimo` · `vlr_financiamento` · `mvalor_financiamento` · `valor_global_de_venda_vgv` |
| `valor_liberado` | `valor_total_liberado` · `mvalor_desembolso` |
| `subsidio_fgts` | `subsidio_fgts` · `siaci_valorsubsidio_fgts` · `vlr_subsidio_fgts` · `complemento_fgts` |
| `subsidio_ogu` | `subsidio_ogu` · `siaci_valorsubsidio_ogu` · `vlr_subsidio_ogu` · `complemento_ogu` |
| `percentual_execucao_fisica` | `obra_executada` · `de_obra_executada` · `prc_execucao_obra` · `percentual_de_obra` · `vfaixa_perc_obra` · `obra` |
| `dt_contratacao` | `data_contratacao` · `dat_contratacao` · `data_da_contratacao_bb` |
| `dt_entrega` | `data_conclusao` · `dat_entregue` · `entrega_do_empreendimento` |

`coalesce_present(relation, aliases)` (nova macro) monta o `coalesce()` só com as
colunas que **existem de fato** na bronze materializada — cada família tem 2-3
gerações de schema e uma coluna pode não existir em nenhum arquivo do lote.

### Cobertura (validada nas amostras locais de 200 linhas/arquivo)

| Família | Meses | Janela | Observação |
|---|---:|---|---|
| `bases_relatorio_executivo` | 44 | 2012-07 → 2018-08 | Mais rica: contratadas + entregues + **split OGU/FGTS** + APF + território |
| `min_cidades` | 25 | 2011-03 → 2016-07 | BB; pj (empreendimento) + pf (contrato). Sem UF (derivar do IBGE). Antigos 2011-13 sem grão útil → filtrados. |
| `entrada_bb` | 22 | 2012-10 → 2014-09 | BB; sem contagem de entregues (`entrega_do_empreendimento` é data). |
| `bext` | 21 | 2012-04 → 2018-08 | CAIXA; grão contrato PF (11M linhas na base real). Só `mvalor_subsidio` total, sem split OGU/FGTS. |

**Extração de `dt_referencia`:** `report_date` → `YYYYMMDD`/`DDMMYYYY` → `YYYY_MM` →
`<mês-abrev pt-BR><ano>` (ex. `abr2018`, `dez17`). Nas amostras: **0 linhas sem
`dt_referencia`** após a cadeia completa (só 1 arquivo `_024_10_...entrega...` de
nome truncado fica sem — coberto por `report_date` na base real).

---

## Config `dbt_project.yml`

```yaml
    indicadores_mcmv_dbt:
      bronze: { +enabled: "target.type == 'duckdb'" }   # + bronze_reloginho_snh_entregas_evento
      silver: { +enabled: "target.type == 'duckdb'" }   # + silver_reloginho_snh_entregas_mes
      gold:
        indicadores_reloginho_entregas: { +enabled: "target.type == 'duckdb'" }

    mcmv_historico_dbt:
      serie_executiva:
        +materialized: table
        +schema: mcmv_historico
        +enabled: "{{ target.type == 'duckdb' }}"
```

## Macros novas

| Macro | Uso |
|---|---|
| `parse_hist_double` / `parse_hist_bigint` / `parse_hist_date` | Parse defensivo (dot-decimal do dump + resíduo BR + `None`/`nan`). |
| `hist_dt_referencia(report_date, filename)` | Mês-snapshot com 5 estratégias de fallback (inclui mês por extenso pt-BR). |
| `coalesce_present(relation, aliases, cast_type)` | `coalesce()` só com colunas existentes na relação (checagem em tempo de compilação). |

## Validação executada

- `dbt parse` + `dbt compile --target staging_duckdb` — OK (104 modelos, 130 testes).
- `dbt ls` — modelos habilitados só no `staging_duckdb`, pulados no `prod` (correto).
- **Teste local end-to-end** com DuckDB 1.5.5 sobre `data/dados_historicos_formatados/table_samples`
  (amostras de 200 linhas): bronze → silver → gold dos dois tiers rodam; `dt_referencia`
  sem nulos; `hash_linha` único; `GROUPING SETS` produz nacional + uf; classificação
  OGU/FGTS funciona; dedup de eventos e de snapshots funciona.
- **Pendente** (requer credencial MinIO): `dbt run` + `dbt test` da linhagem completa
  contra o `staging/dados_historicos` real, e reconciliação dos totais contra os
  relatórios executivos oficiais da época.

```bash
cd airflow_lappis/dags/dbt/mcid
export MINIO_ENDPOINT=... MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... MINIO_BUCKET=data-lake-mcid
export DUCKDB_MCID_PATH=/tmp/mcid_staging.duckdb
dbt run  --target staging_duckdb --select bronze_reloginho_snh_entregas_evento+ bronze_mcmv_serie_executiva_historica+
dbt test --target staging_duckdb --select bronze_reloginho_snh_entregas_evento+ bronze_mcmv_serie_executiva_historica+
```

## Pendências / próximos passos

1. **Rodar contra o MinIO real** e reconciliar totais por família contra os
   relatórios executivos oficiais (validação de negócio — Fase 5/6).
2. **Ajuste fino do mapa de colunas**: se o `dbt run` falhar com "column X not
   found", `coalesce_present` já protege; se um valor vier errado, revisar o alias
   no `silver_mcmv_serie_executiva_historica.sql`.
3. **Consolidação de série única**: criar um mart que aplica `prioridade_familia`
   por `(dt_referencia, uf)` — hoje o gold expõe todas as famílias.
4. **Aposentar o seed do piloto #118**: `historico_mcmv_serie_temporal_snapshot`
   passa a ler `gold_mcmv_serie_historica_mensal` filtrado por `linha_ogu_fgts`
   (anualizando) em vez de `issue_118_mcmv_serie_temporal_piloto.csv`.
5. **`entrada_bb` / `bext` sem UF ou sem split OGU/FGTS** — documentado; usar as
   outras famílias quando o recorte exigir.
6. **Faixa 3 / PMCMV-3** (`pmcmv_3_relatório_executivo`, 2015-2018) e o dump
   relacional BB 2013 — 5ª/6ª famílias, follow-up (grupo C).
