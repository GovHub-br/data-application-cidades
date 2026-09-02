# Issue #130 — Reconciliacao do Reloginho com Dados Historicos (staging/dados_historicos)

> Data: 2026-08-31. Fonte: parquets canonicos em `staging/dados_historicos/` (dump
> tratado convertido de CSV para Parquet pelo change `reloginho-dados-historicos`).
> Verificacao executada via DuckDB local com o mesmo SQL que hoje esta distribuido
> entre `bronze_reloginho_snh_serie_mensal` (ingestao + dt_referencia do filename),
> `silver_reloginho_snh_apf_mes` (dedup por APF) e o gold
> `indicadores_mcmv_dbt/gold/indicadores_reloginho.sql` (soma mensal). Referencia:
> validacao da Fase 4 da #130.
>
> **Atualizacao 2026-09-02:** quando esta verificacao foi feita, o SQL estava num
> unico arquivo gold. A refatoracao em camadas preserva a chave de dedup
> `(agente_financeiro, apf, dt_referencia)` e a saida do gold — a reconciliacao
> abaixo segue valida e virou o teste `assert_reloginho_reconcilia_66`.

## Resultado da reconciliacao (CAIXA 2026-03)

| Indicador | Calculado | Referencia #66 | Diff |
|---|---:|---:|---:|
| `uh_contratadas` | 1.697.630 | 1.697.630 | 0,000% |
| `uh_entregues` | 1.391.909 | 1.391.909 | 0,000% |

**Status: PASS** (tolerancia de ±0,5% — diffs zeradas).

## Serie mensal SNH consolidada (deduplicada por APF)

CAIXA (contratadas / entregues, 22 meses 2024-06 a 2026-03):

| Mes | Contratadas | Entregues |
|---|---:|---:|
| 2024-06 | 1.483.886 | 1.359.712 |
| 2024-12 | 1.575.463 | 1.365.758 |
| 2025-06 | 1.644.494 | 1.371.366 |
| 2025-12 | 1.688.773 | 1.382.462 |
| 2026-03 | 1.697.630 | 1.391.909 |

BB 2026-03: `uh_contratadas` = 167.762 (bate com a referencia #66); `uh_entregues` = 146.976.

## Observacoes

1. **Deduplicacao 2x por APF confirmada e neutralizada**: `historico_recente_*` traz cada
   APF exatamente 2x (ex. CAIXA 2026-03: 29.906 linhas = 14.953 APFs x2). Sem `row_number()`
   os totais dobrariam. A dedup esta aplicada na `silver_reloginho_snh_apf_mes` (era no
   gold quando esta verificacao foi feita) e validada aqui.
2. **Serie BB com lacunas**: alguns meses BB (`202406`, `202410`, `202411`, `202501`) vem
   com `uh_contratadas`/`uh_entregues` vazios na origem (totais 0). Nao afeta a reconciliacao
   CAIXA (referencia da #130), mas deve ser sinalizado para a serie BB.
3. **Nomes truncados na origem**: `storico_recente_2024_07_*_af_bb_vs02` e
   `ecente_2024_07_*_af_bb_vs02_correcao` (prefixo `historico_recente_` truncado na fonte).
   O glob antigo `historico_recente_*.parquet` nao os capturava; o glob atual da bronze
   (`*ecente_*snh_pmcmv_dados_prioritarios_af_*`) captura, e a variante `correcao` vence a
   `vs02` pela `prioridade_reentrega`. Impacto restrito a BB (1 mes); serie CAIXA inalterada.
4. **Ritmo medio mensal** (via `resumo_reloginho_dashboard`): CAIXA = 1.391.909 / 22 ≈ 63.268
   UH/mes (janela de observacao a confirmar na Fase 5).

## Pendencias (fora do escopo deste change)

- Execucao `dbt run --target staging_duckdb --select bronze_reloginho_snh_serie_mensal+`
  + `dbt test` em ambiente com credencial MinIO (validado ate `dbt compile`).
- Serie BB: meses `202406`, `202410`, `202411`, `202501` vem vazios na origem;
  6 meses BB sem arquivo `historico_recente_*` (2024-08/09/12, 2025-02/04/11);
  2024-08 sem arquivo em nenhum agente. Detalhe em
  `issue-130-refatoracao-medalhao-reloginho.md` (Parte 2).
- Meta oficial do ciclo e indicadores dela dependentes (`perc_meta_*`, `gap_uh_meta`,
  `ritmo_necessario`, `projecao_entrega`, `status_relogio`) — decisao de negocio (Fase 5).
- Ponteiro SNH "atual" (202606) — nao esta no dump.
