# Inventario Superset x SharePoint x MinIO - MCMV

- Gerado em: `2026-08-19 02:45:34 UTC`
- Datasets Superset: `124`
- Charts Superset: `0`
- Dashboards Superset: `6`

## Resumo de datasets por frente inferida

| Frente | Datasets |
|---|---:|
| cidades | 106 |
| far | 8 |
| rural | 7 |
| entidades | 3 |

## Existencia das frentes em SharePoint/MinIO

| Frente | SharePoint tem frente | MinIO bucket tem frente | MinIO staging tem frente | Datasets |
|---|---|---|---|---:|
| cidades | False | False | False | 106 |
| far | True | True | False | 8 |
| rural | True | True | False | 7 |
| entidades | True | True | False | 3 |

## Evidencias geradas

- `superset_datasets.csv`: datasets publicados no Superset.
- `superset_charts.csv`: graficos publicados no Superset.
- `superset_dashboards.csv`: dashboards publicados no Superset.
- `superset_datasets_vs_sharepoint_minio.csv`: batimento dataset a dataset.
- `superset_resumo_datasets_por_frente.csv`: resumo por frente inferida.
- `superset_resumo_existencia_por_frente.csv`: resumo de existencia por frente.

Observacao: match por nome exige nomes fisicos semelhantes. Quando nao houver match por nome, use `*_tem_frente` como sinal de cobertura semantica, nao como prova de equivalencia arquivo-tabela.
