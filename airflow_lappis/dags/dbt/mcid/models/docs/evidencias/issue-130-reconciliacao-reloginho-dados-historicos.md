# Issue #130 — Reconciliacao do Reloginho com Dados Historicos (staging/dados_historicos)

> Data: 2026-08-31. Fonte: parquets canonicos em `staging/dados_historicos/` (dump
> tratado convertido de CSV para Parquet pelo change `reloginho-dados-historicos`).
> Verificacao executada via DuckDB local com o MESMO SQL do modelo gold
> `indicadores_mcmv_dbt/gold/indicadores_reloginho.sql` (dedup por APF + dt_referencia
> do filename). Referencia: validacao da Fase 4 da #130.

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
   os totais dobrariam. A dedup esta aplicada no gold e validada aqui.
2. **Serie BB com lacunas**: alguns meses BB (`202406`, `202410`, `202411`, `202501`) vem
   com `uh_contratadas`/`uh_entregues` vazios na origem (totais 0). Nao afeta a reconciliacao
   CAIXA (referencia da #130), mas deve ser sinalizado para a serie BB.
3. **Nomes truncados na origem**: `storico_recente_2024_07_*_af_bb_vs02` e
   `ecente_2024_07_*_af_bb_vs02_correcao` (prefixo `historico_recente_` truncado na fonte)
   nao sao capturados pelo glob `historico_recente_*.parquet` do gold. Impacto restrito a BB
   (1 mes, variante vs02); a serie CAIXA (referencia) esta completa.
4. **Ritmo medio mensal** (via `resumo_reloginho_dashboard`): CAIXA = 1.391.909 / 22 ≈ 63.268
   UH/mes (janela de observacao a confirmar na Fase 5).

## Pendencias (fora do escopo deste change)

- Execucao `dbt run --target staging_duckdb --select indicadores_reloginho+` + `dbt test`
  em ambiente com o venv dbt correto (o binario standalone dbt-fusion 2.0.0-preview.212 do
  ambiente atual nao executa de forma confiavel e o projeto ja carrega 56 erros de
  validacao YAML pre-existentes).
- Meta oficial do ciclo e indicadores dela dependentes (`perc_meta_*`, `gap_uh_meta`,
  `ritmo_necessario`, `projecao_entrega`, `status_relogio`) — decisao de negocio (Fase 5).
- Ponteiro SNH "atual" (202606) — nao esta no dump.
