# Issue #66 - Fontes MinIO Para Relogio de Metas e Alertas

## Contexto

A varredura foi feita no bucket `data-lake-mcid`, endpoint `10.0.0.56:9000`,
com acesso somente leitura pela VPN. O objetivo foi identificar bases historicas
que ajudam a montar o relogio de unidades habitacionais e os alertas de gargalo
para garantir a entrega do MCMV.

## Achados Principais

| Achado | Evidencia |
|---|---:|
| Objetos listados no bucket | 6.697 |
| Tamanho total aproximado | 297.518 MB |
| Arquivos em `raw/` | 2.703 |
| Arquivos em `staging/` | 3.958 |
| CSVs historicos em `raw/dados_historicos/` | 754 |
| Arquivos com sinal de dados prioritarios SNH | 619 |
| Arquivos com sinal de entregas de UH | 278 |
| Arquivos com sinal de execucao fisica | 1.134 |
| Arquivos com sinal financeiro/desembolso | 242 |
| Arquivos com sinal FGTS/SBPE | 469 |
| Arquivos com sinal FAR | 640 |
| Arquivos com sinal FDS/Entidades | 466 |

## Fontes Para o Relogio

Para o relogio, existem dois ponteiros possiveis. A imagem de referencia fala em
`Total Contratacoes`; a conversa tambem menciona unidades entregues. Portanto a
recomendacao e manter os dois KPIs lado a lado:

| KPI | Fonte recomendada | Data do movimento | Total observado |
|---|---|---:|---:|
| UH contratadas CAIXA + BB | `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.csv` + `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.txt` | 30/06/2026 | 1.874.623 |
| UH entregues CAIXA + BB | campos `UH Entregues` nas mesmas bases mensais | 30/06/2026 | 1.543.432 |
| UH entregues por evento | `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA_ENTREGAS.csv` + `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_DA_ENTREGA_DA_UNIDADE_AF_BB.csv` | 30/06/2026 | 1.518.598 |
| Contratacoes semanais FAR/FDS/Rural | `raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_*.csv` | 15/08/2025 a 31/07/2026 | 112.869 |

Usando a meta visual de `2.214.810` UHs:

| Ponteiro | Valor | Progresso sobre 2.214.810 |
|---|---:|---:|
| Contratadas CAIXA + BB em 30/06/2026 | 1.874.623 | 84,64% |
| Entregues CAIXA + BB em 30/06/2026 | 1.543.432 | 69,69% |

Observacao: os arquivos `raw/dados_prioritarios_recebidos_*` existem, mas estavam
com movimento de janeiro/2026. Para relogio atual, a base mensal `202606_*` e
mais adequada.

## Fontes Para Alertas

As bases mensais de junho/2026 tem os campos necessarios para alertas
operacionais por APF:

- `Data de Movimento`
- `Agente Financeiro`
- `APF`
- `UF`
- `Municipio`
- `Modalidade`
- `Situacao do Empreendimento`
- `% Exec`
- `Valor Contratado`
- `Valor Desembolsado`
- `UH Contratadas`
- `UH Entregues`
- `UH Vigentes`
- `Data da previsao da entrega`
- latitude/longitude

Alertas simples calculados na amostra mensal CAIXA + BB de 30/06/2026:

| Alerta | Resultado |
|---|---:|
| APFs com previsao vencida, UH vigente e execucao menor que 100% | 58 |
| UHs vigentes associadas a APFs atrasadas | 13.944 |
| APFs vigentes com execucao abaixo de 70% | 2.449 |
| UHs vigentes com baixa execucao | 194.015 |

Distribuicao das UHs contratadas na base mensal:

| Modalidade | APFs | UH contratadas | UH entregues | Media % exec |
|---|---:|---:|---:|---:|
| FAR | 4.697 | 1.507.878 | 1.301.404 | 88,58 |
| RURAL | 9.636 | 242.567 | 181.844 | 88,76 |
| Entidades | 890 | 104.010 | 42.603 | 60,72 |
| Rural | 1.088 | 20.168 | 17.581 | 97,02 |

## Dados Historicos Adicionais Relevantes

- `raw/dados_historicos/`: 754 CSVs historicos; importante para treino de
  sazonalidade, comportamento por periodo, mudancas de layout e validacao de
  tendencia.
- `staging/`: 3.958 parquets derivados; importante para consumo analitico mais
  rapido quando o ambiente tiver `pyarrow`/engine parquet disponivel.
- `raw/fgts_canal_tab_ao_*`: fontes grandes para Faixa 2, Faixa 3/FGTS e
  possiveis gargalos financeiros/execucao de FGTS.
- `raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_*`: serie
  incremental semanal que ajuda a medir ritmo recente de contratacoes.
- `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_*`: melhores fontes atuais para
  ponteiro e alertas por APF, UF, modalidade e agente financeiro.

Nao foram encontrados arquivos com `meta`, `ciclo` ou `retomada` no nome. A meta
do relogio deve ser carregada como parametro/tabela de metas oficial, em vez de
inferida do MinIO.

## Evidencias Geradas

- `docs/evidencias/issue-66-minio-inventario-preditivo.csv`
- `docs/evidencias/issue-66-minio-inventario-preditivo.md`
- `docs/evidencias/issue-66-minio-amostras-cabecalho.csv`
- `docs/evidencias/issue-66-minio-bases-relogio-alertas-resumo.csv`
- `docs/evidencias/issue-66-minio-202606-caixa-bb-perfil-alertas.csv`
- `docs/evidencias/issue-66-minio-contratacoes-semanais-modalidade.csv`
- `docs/evidencias/issue-66-minio-entregas-atuais-resumo.csv`
- `docs/evidencias/issue-66-minio-serie-contratacoes-semanais.csv`
- `docs/evidencias/issue-66-frentes-contratos-valores-agregados.csv`
- `docs/evidencias/issue-66-frentes-arquivos-monitorados.csv`
- `docs/evidencias/issue-66-frentes-top-ufs.csv`
- `docs/evidencias/issue-66-fnhis-fontes-propostas.csv`
- `docs/evidencias/issue-66-matriz-frentes-dashboard.csv`

## Matriz Por Frente

A leitura detalhada por programa/frente esta documentada em
`docs/issue-66-matriz-frentes-relogio-alertas.md`. Ela separa MCMV ciclo
2023-2026, FAR, Entidades/FDS, Rural, FNHIS, Faixa 3/FGTS, Faixa 2/FGTS/SBPE,
Reforma Casa Brasil, dados historicos e SFTP operacional.

## Recomendacao Para a Proxima Modelagem

1. Criar uma tabela pequena de metas oficiais por ciclo, faixa, frente e ano.
2. Criar uma gold `relogio_mcmv_metas` com contratado, entregue, vigente,
   meta, progresso, desvio e ritmo necessario.
3. Reaproveitar `indicadores_gargalo_desempenho` para alertas por APF e criar
   uma visao de alertas priorizados por impacto em UH vigente.
4. Incorporar as bases FGTS quando o dashboard precisar cobrir Faixa 2/3 e
   SBPE alem de FAR/FDS/Rural.
