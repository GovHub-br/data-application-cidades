# Issue #66 — Entrega: Indicadores Historicos, Relogio de Metas e Alertas

## Resumo para colar na issue

Foi consolidada a base tecnica para os indicadores de gargalo/desempenho e para o relogio de metas do MCMV. A entrega cruza dados atuais do MinIO/SFTP com serie historica de contratacoes, separando corretamente OGU/subsidiado de FGTS/financiado para evitar leituras erradas sobre a janela historica.

O objetivo do relogio e monitorar, por frente e no total, o andamento ate a meta de unidades habitacionais, enquanto os alertas apontam risco de atraso, baixa execucao, gargalo financeiro, territorio critico e falta de atualizacao.

## O que foi entregue

- Documento tecnico de indicadores de gargalo e desempenho.
- Matriz de frentes, KPIs do relogio, fontes principais e alertas prioritarios.
- Inventario de fontes MinIO relevantes para metas, entregas, execucao fisica, financeiro/desembolso e historico.
- CSVs de evidencia com contagens, UHs, arquivos e fontes.
- Serie temporal historica MCMV separando OGU/subsidiado e FGTS/financiado.
- Graficos em PNG para uso em apresentacao, issue ou relatorio.
- Regras iniciais para cards, filtros e visuais de dashboard.

## Evidencias principais

- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/issue-66-indicadores-gargalo-desempenho.md`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/issue-66-fontes-minio-relogio-alertas.md`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/issue-66-matriz-frentes-relogio-alertas.md`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-matriz-frentes-dashboard.csv`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-minio-bases-relogio-alertas-resumo.csv`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-minio-entregas-atuais-resumo.csv`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-uh-contratadas.csv`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-ogu-barras-fgts-linha.png`

## Fontes atuais do relogio

As fontes atuais monitoradas para o relogio de metas incluem:

| Fonte | Linhas | APFs | Campo monitorado | Data | UH |
|---|---:|---:|---|---|---:|
| `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA_ENTREGAS.csv` | 11.654 | 11.653 | `QT_UH_ENTREGUES` | 30/06/2026 | 1.395.669 |
| `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_DA_ENTREGA_DA_UNIDADE_AF_BB.csv` | 5.961 | 167 | `Número de Unidades Entregues` | 30/06/2026 | 122.929 |
| `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.csv` | 15.023 | 15.023 | `UH Contratadas` | 2026-06-30 | 1.706.861 |
| `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.txt` | 1.288 | 1.288 | `UH Contratadas` | 30/06/2026 | 167.762 |

Leitura pratica: nas bases atuais monitoradas, o relogio pode usar contratadas e entregues por agente financeiro, frente, UF, municipio e APF. O total final deve ser calculado com regra de deduplicacao por APF/contrato para evitar dupla contagem entre arquivos de contratacao e entrega.

## Frentes cobertas

- FAR: UH contratadas, UH entregues, UH vigentes, atraso, baixa execucao e gargalo financeiro.
- Entidades/FDS: UH contratadas, UH entregues, execucao fisica, divergencia fisico-financeira e entrega em risco.
- Rural: UH contratadas, UH entregues, ritmo insuficiente, atraso e falta de atualizacao.
- FNHIS/SUB50: propostas localizadas; regra de conversao para UH ainda pendente.
- Classe Media/Faixa 3: contratos, valor financiado, territorio critico e meta por faixa.
- FGTS/SBPE: contratos, desembolso, execucao e andamento de obra.
- Reforma Casa Brasil: numero de contratos, valor financiado, ritmo e concentracao territorial.
- Historico preditivo: series historicas para tendencia, sazonalidade, drift e backtest.

## Indicadores do relogio

- `uh_meta_total`: meta oficial de unidades habitacionais do ciclo.
- `uh_contratadas`: unidades contratadas ate a data de referencia.
- `uh_entregues`: unidades entregues ate a data de referencia.
- `perc_meta_contratada`: `uh_contratadas / uh_meta_total`.
- `perc_meta_entregue`: `uh_entregues / uh_meta_total`.
- `gap_uh_meta`: `uh_meta_total - uh_entregues`.
- `ritmo_medio_mensal`: entregas acumuladas divididas pelos meses observados.
- `ritmo_necessario`: unidades restantes divididas pelos meses restantes do ciclo.
- `projecao_entrega`: entregas observadas mais ritmo recente projetado.
- `status_relogio`: `No prazo`, `Atencao` ou `Risco`.

## Alertas candidatos

- Meta em risco: projecao de entrega menor que meta oficial.
- Ritmo insuficiente: ritmo recente abaixo do ritmo necessario.
- Obra sem atualizacao recente: data de referencia vencida ou sem movimento.
- Baixa execucao fisica: execucao fisica abaixo do esperado para idade do contrato.
- Baixa execucao financeira: desembolso abaixo da execucao fisica ou saldo represado.
- Gargalo financeiro: valor liberado/desembolsado distante do contratado.
- Contrato sem evolucao: sem alteracao fisica/financeira em janela definida.
- Territorio critico: UF/municipio com concentracao de entregas em risco.
- Responsavel critico: empresa, entidade, agente ou ente com score alto de gargalo.

## Leitura historica correta

A serie historica mostra que OGU/subsidiado e FGTS/financiado precisam ser analisados separadamente. No periodo recente, a linha OGU/subsidiado fica zerada ou muito baixa em alguns anos, enquanto FGTS/financiado continua com volume. Portanto, o dashboard nao deve concluir que nao houve nenhuma contratacao habitacional; deve apontar que houve queda/hiato na contratacao subsidiada OGU, mantendo FGTS como linha distinta de contexto.

## Proximo passo para dashboard

Criar a gold/mart do relogio a partir da silver padronizada e das fontes atuais, com grao minimo:

- ciclo
- frente
- ano_mes_referencia
- UF
- municipio
- agente financeiro
- APF/contrato quando aplicavel

A mart deve alimentar cards de meta, progresso, gap, ritmo necessario, projecao e alertas por frente/territorio/responsavel.
