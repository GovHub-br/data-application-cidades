# Issue #66 - Matriz Por Frente Para Relogio, Gargalos e Alertas

## Objetivo

Consolidar, por frente/programa, quais dados historicos e atuais entram no
relogio de metas e quais campos sustentam os alertas preditivos. Esta matriz
deve orientar a proxima modelagem gold e o dashboard operacional.

## Visao Executiva

| Programa | Frente | Status do dado | Melhor fonte atual | Ponteiro principal | Alerta principal |
|---|---|---|---|---|---|
| MCMV ciclo 2023-2026 | FAR | Pronto para dashboard | `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.csv` + BB | UH contratadas, UH entregues, UH vigentes | atraso, baixa execucao, saldo financeiro |
| MCMV ciclo 2023-2026 | Entidades/FDS | Pronto com ressalva de baixa execucao | bases `202606_*` + gold FDS | UH contratadas, UH entregues, execucao fisica | baixa execucao e entrega em risco |
| MCMV ciclo 2023-2026 | Rural | Pronto com normalizacao de rotulo | bases `202606_*` + serie semanal FAR/FDS/Rural | UH contratadas e entregues | ritmo insuficiente e atraso |
| MCMV ciclo 2023-2026 | FNHIS | Fonte localizada, precisa regra oficial | `novo_mcmv_fnhis_sub_50_*` | propostas apresentadas/selecionadas ou meta oficial | pendencia de conversao para UH/contrato |
| MCMV financiado | Faixa 3/FGTS | Fonte atual localizada | `raw/PMCMV_FAIXA3_MCID_2026_07_31.csv` | contratos e valor financiado | atraso, risco financeiro, territorio |
| MCMV financiado | Faixa 2/FGTS/SBPE | Fonte candidata localizada | `raw/fgts_canal_tab_ao_*` e snapshots FGTS | contratos, desembolso, execucao | gargalo financeiro e andamento de obra |
| Reforma Casa Brasil | Reforma - contratos | Fonte consolidada e snapshot atual | `raw/reforma_casa_brasil_contratacao.csv` + `raw/PMCMV_REFORMAS_MCID_2026_07_31.csv` | numero de contratos | ritmo contra meta e atraso |
| Reforma Casa Brasil | Reforma - valor | Fonte consolidada e snapshot atual | mesmas bases de Reforma | `vr_evento` ou `vr_investimento` | valor contratado abaixo da meta |
| Todas | Historico/treino | Pronto para backtest | `raw/dados_historicos/` e `staging/` | serie temporal por APF/data | sazonalidade, tendencia e drift |
| Todas | SFTP operacional | Pronto para alertas recentes | inventario SFTP 2020-2026 | snapshots por data de movimento | mudanca de status, sem atualizacao |

## MCMV Ciclo 2023-2026

### Relogio Geral

Fonte atual recomendada:

- `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.csv`
- `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.txt`

Referencia calculada em 30/06/2026:

| Ponteiro | Valor | Meta visual | Progresso |
|---|---:|---:|---:|
| UH contratadas CAIXA + BB | 1.874.623 | 2.214.810 | 84,64% |
| UH entregues CAIXA + BB | 1.543.432 | 2.214.810 | 69,69% |
| UH vigentes | 313.884 | n/a | n/a |

Leitura: o ponteiro de contratacao esta mais proximo da meta, mas o ponteiro de
entrega ainda precisa ser exibido separadamente para evitar uma leitura otimista
demais. Para tomada de decisao, o relogio deve mostrar `contratadas`,
`entregues`, `vigentes`, `meta` e `gap_para_meta`.

### Frente FAR

| Indicador | Valor em 30/06/2026 |
|---|---:|
| APFs | 4.697 |
| UH contratadas | 1.507.878 |
| UH entregues | 1.301.404 |
| Media de execucao | 88,58% |

Uso no dashboard:

- Relogio: UH contratadas e UH entregues.
- Alerta: APF com previsao vencida, execucao menor que 100% e UH vigente.
- Corte prioritario: UF, municipio, agente financeiro, situacao, faixa.

### Frente Entidades/FDS

| Indicador | Valor em 30/06/2026 |
|---|---:|
| APFs | 890 |
| UH contratadas | 104.010 |
| UH entregues | 42.603 |
| Media de execucao | 60,72% |

Leitura: esta frente e a mais sensivel do recorte atual, porque combina baixa
entrega relativa com execucao media menor. Deve entrar como bloco proprio no
dashboard, nao apenas agregado ao MCMV.

Alertas recomendados:

- baixa execucao fisica;
- baixa execucao financeira;
- obra sem atualizacao recente;
- divergencia fisico-financeira;
- responsavel com muitos casos em risco.

### Frente Rural

No arquivo mensal, a frente aparece com dois rotulos: `RURAL` e `Rural`.
Eles devem ser normalizados antes da gold.

| Rotulo no dado | APFs | UH contratadas | UH entregues | Media de execucao |
|---|---:|---:|---:|---:|
| RURAL | 9.636 | 242.567 | 181.844 | 88,76% |
| Rural | 1.088 | 20.168 | 17.581 | 97,02% |
| Rural normalizado | 10.724 | 262.735 | 199.425 | n/a |

Uso no dashboard:

- Relogio: UH contratadas/entregues por UF e municipio.
- Alerta: ritmo insuficiente de entrega, atraso e falta de atualizacao.
- Pendencia: padronizar `Modalidade` para evitar duplicidade visual.

### Frente FNHIS

Fontes localizadas:

- `raw/novo_mcmv_fnhis_sub_50_propostas_apresentadas.csv`
- `raw/novo_mcmv_fnhis_sub_50_propostas_selecionadas.csv`

Leitura: a fonte existe, mas a granularidade e de proposta. Para o relogio, a
regra precisa dizer se o ponteiro e proposta, contrato ou UH. A meta da imagem
deve entrar por tabela parametrizada, nao por inferencia do arquivo.

## MCMV Financiado: Faixa 3/FGTS e Faixa 2/FGTS/SBPE

### Faixa 3/FGTS

Fonte atual recomendada:

- `raw/PMCMV_FAIXA3_MCID_2026_07_31.csv`

Agregado por ano/faixa:

| Ano | Faixa | Contratos | `vr_evento` | `vr_investimento` |
|---:|---|---:|---:|---:|
| 2025 | 003 | 44.001 | 8,82 Bi | 12,33 Bi |
| 2026 | 003 | 89.149 | 19,10 Bi | 26,52 Bi |
| Total | 003 | 133.150 | 27,93 Bi | 38,85 Bi |

Leitura: esta frente deve usar contratos e valor financiado, nao UH, ate que
haja uma regra oficial de equivalencia para unidade habitacional.

### Faixa 2/FGTS/SBPE

Fontes candidatas:

- `raw/fgts_canal_tab_ao_1_contratos_fgts.csv`
- `raw/fgts_canal_tab_ao_2_tab_desembolsos_fgts.csv`
- `raw/fgts_canal_tab_ao_2_tab_execucoes_obras.csv`
- `raw/fgts_canal_tab_ao_3_acompanhamento_termino_obra.csv`
- snapshots `gefus_anteriores_base_pf_fgts_*`

Leitura: os arquivos FGTS sao grandes e detalhados. Devem virar uma frente
propria de modelagem, com ingestao incremental e gold agregada por contrato,
UF, municipio, faixa, status e data de referencia.

## Reforma Casa Brasil

### Base Consolidada

Fonte:

- `raw/reforma_casa_brasil_contratacao.csv`

Esta base bate com o print para 2025:

| Ano | Faixa | Contratos | `vr_evento` | `vr_investimento` |
|---:|---|---:|---:|---:|
| 2025 | 001 | 28.295 | 380,5 Mi | 422,6 Mi |
| 2025 | 002 | 8.352 | 151,0 Mi | 167,7 Mi |
| 2025 | Total | 36.647 | 531,4 Mi | 590,3 Mi |
| 2026 | 001 | 10.108 | 169,9 Mi | 169,9 Mi |
| 2026 | 002 | 4.757 | 97,2 Mi | 97,3 Mi |
| 2026 | Total | 14.865 | 267,0 Mi | 267,2 Mi |

O print recebido mostra 16.708 contratos em 2026 e 833,0 Mi no total. A base
consolidada calculada aqui chega a 51.512 contratos e 798,5 Mi em `vr_evento`.
A diferenca sugere que o print usa um corte mais recente ou outra regra de valor.

### Snapshot Atual SFTP

Fonte:

- `raw/PMCMV_REFORMAS_MCID_2026_07_31.csv`

Agregado:

| Ano | Faixa | Contratos | `vr_evento` | `vr_investimento` |
|---:|---|---:|---:|---:|
| 2025 | 001 | 28.292 | 422,5 Mi | 422,6 Mi |
| 2025 | 002 | 8.228 | 165,2 Mi | 165,2 Mi |
| 2026 | 001 | 76.452 | 2,20 Bi | 2,20 Bi |
| 2026 | 002 | 13.435 | 272,8 Mi | 273,2 Mi |
| Total | 001/002 | 126.407 | 3,06 Bi | 3,06 Bi |

Leitura: para o dashboard operacional, usar o snapshot SFTP mais recente. Para
reproduzir o print institucional de 02/03/2026, usar a base consolidada ou uma
tabela de corte com a mesma data de posicao.

## Alertas Padrao Por Frente

| Alerta | Regra sugerida | Frentes |
|---|---|---|
| Atraso | previsao vencida e execucao menor que 100% | FAR, FDS, Rural |
| Baixa execucao | execucao menor que limiar da frente ou menor que curva esperada | FAR, FDS, Rural, FGTS |
| Gargalo financeiro | saldo/desembolso incompativel com execucao fisica | FAR, FDS, FGTS |
| Ritmo insuficiente | ritmo recente menor que necessario para fechar meta no prazo | todas com meta |
| Sem atualizacao | data de movimento ou medicao antiga | todas |
| Responsavel critico | responsavel com muitos contratos/APFs em risco | FAR, FDS, Reforma, FGTS |
| Territorio critico | UF/municipio com maior gap para meta ou maior volume em risco | todas |

## Regras Para a Gold do Relogio

A gold deve ter um formato unico, independente do programa:

| Campo | Descricao |
|---|---|
| `programa` | MCMV, Reforma Casa Brasil, FGTS etc. |
| `frente` | FAR, Entidades, Rural, FNHIS, Faixa 3, Reforma |
| `kpi` | contratos, valor financiado, UH contratadas, UH entregues |
| `data_referencia` | data do snapshot ou movimento |
| `valor_realizado` | total observado |
| `meta` | meta oficial parametrizada |
| `percentual_meta` | `valor_realizado / meta` |
| `gap_meta` | `meta - valor_realizado` |
| `ritmo_recente` | media movel semanal/mensal |
| `ritmo_necessario` | gap dividido pelo tempo restante |
| `status_meta` | no ritmo, atencao, critico |

## Evidencias Novas

- `docs/evidencias/issue-66-frentes-contratos-valores-agregados.csv`
- `docs/evidencias/issue-66-frentes-arquivos-monitorados.csv`
- `docs/evidencias/issue-66-frentes-top-ufs.csv`
- `docs/evidencias/issue-66-fnhis-fontes-propostas.csv`
- `docs/evidencias/issue-66-matriz-frentes-dashboard.csv`

## Decisoes Pendentes

1. Definir tabela oficial de metas por programa/frente/ano.
2. Confirmar se `valor financiado` deve usar `vr_evento`, `vr_investimento` ou
   outro campo oficial.
3. Confirmar a regra de FNHIS: proposta, contrato ou UH.
4. Normalizar `RURAL`/`Rural` e nomes de faixa (`001`, `002`, `003`).
5. Separar dashboards: relogio executivo e mesa operacional de alertas.
