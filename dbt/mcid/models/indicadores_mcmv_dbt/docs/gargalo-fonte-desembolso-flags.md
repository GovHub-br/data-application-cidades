# `ouro_reloginho_indicadores_gargalo_desempenho` — fonte de desembolso e flags

Change `vocabulario-e-qualidade-financeira-historica`, tarefa 6.4.

## O que mudou (D4)

`valor_liberado_historico` deixou de ser
`coalesce(<agregado *_financeiro_mensal SharePoint>, <ficha GEFUS/CAIXA>)` e
passou a ser **só a ficha** (`= valor_desembolsado`). O agregado SharePoint
virou a coluna informativa `valor_desembolsado_componentes` (NULL onde não há
liberação pós-2024 no snapshot).

## Distribuição dos flags — antes × depois (build local 2026-09-06)

| flag | FAR antes | FAR depois | FDS antes | FDS depois |
|---|---|---|---|---|
| `flag_gargalo_financeiro` | 763/822 (93%) | **763/822 (92,8%)** | 314/335 (94%) | **314/335 (93,7%)** |
| `flag_baixa_execucao_financeira` | 601/822 (73%) | **601/822 (73,1%)** | — | 185/335 (55,2%) |

**Os flags NÃO mudaram.** Motivo: a lógica de
`flag_gargalo_financeiro` / `flag_baixa_execucao_financeira` nunca consumiu
`valor_liberado_historico`. Ela usa:

- `percentual_saldo_a_desembolsar` = `(valor_contratado − valor_desembolsado) /
  valor_contratado` — **já baseado na ficha**;
- `percentual_execucao_financeira` — herdado das golds de ficha FAR/FDS,
  também da ficha.

O `coalesce` removido afetava apenas a **coluna de exibição**
`valor_liberado_historico` (agora idêntica a `valor_desembolsado` em 100% das
1.157 linhas).

## Consequência

A expectativa do design ("os flags caem bem abaixo dos 93%") **não se
confirma** — a taxa de 93% é efeito dos **limiares** dos flags
(`percentual_saldo_a_desembolsar >= 30 and percentual_execucao_fisica < 95`),
não da fonte de desembolso. Recalibrar esses limiares é **non-goal** desta
change ("redesenhar `ouro_reloginho_indicadores_gargalo_desempenho` além da fonte de desembolso").
Fica registrado como candidato a change própria.

A mudança entregue continua correta e necessária: elimina o `coalesce` de um
feed parcial (SharePoint, cobertura pós-2024) sobre a fonte oficial, e expõe o
agregado SharePoint só como informação de decomposição, com nome que não
sugere totalidade.
