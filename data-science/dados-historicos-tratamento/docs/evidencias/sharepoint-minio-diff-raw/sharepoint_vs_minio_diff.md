# Diff SharePoint local x MinIO

- Gerado em: `2026-08-19 03:16:32 UTC`
- Prefixos MinIO consultados: `raw/`
- Arquivos candidatos no SharePoint local: `1400`
- Objetos MinIO inventariados: `2762`
- Match forte por nome/stem: `0`
- Pista semantica sem match de nome: `937`
- Nao encontrados no MinIO consultado: `463`
- Objetos MinIO sem match no SharePoint: `2762`

## Como interpretar

- `existe_nome_exato`: arquivo do SharePoint aparece no MinIO com o mesmo nome.
- `existe_mesmo_stem_extensao_diferente`: parece o mesmo artefato convertido, por exemplo CSV/TXT no SharePoint e Parquet no MinIO.
- `pista_semantica_mesma_frente_periodo_fonte`: existe algo no MinIO com mesma frente/periodo/fonte, mas o nome nao comprova equivalencia.
- `nao_encontrado_no_minio_consultado`: o SharePoint complementa o MinIO nos prefixos consultados, ou o arquivo foi renomeado fora das regras de match.

## Resultado por frente

| Frente | Match | Quantidade |
|---|---|---:|
| entidades | pista_semantica_mesma_frente_periodo_fonte | 215 |
| entidades | nao_encontrado_no_minio_consultado | 140 |
| far | pista_semantica_mesma_frente_periodo_fonte | 589 |
| far | nao_encontrado_no_minio_consultado | 109 |
| fgts_entregas | nao_encontrado_no_minio_consultado | 13 |
| fgts_financiado | pista_semantica_mesma_frente_periodo_fonte | 116 |
| fgts_financiado | nao_encontrado_no_minio_consultado | 24 |
| nao_classificado | nao_encontrado_no_minio_consultado | 26 |
| nao_classificado | pista_semantica_mesma_frente_periodo_fonte | 1 |
| ogu_subsidiado | nao_encontrado_no_minio_consultado | 43 |
| rural | nao_encontrado_no_minio_consultado | 108 |
| rural | pista_semantica_mesma_frente_periodo_fonte | 16 |

## MinIO por prefixo/frente

| Prefixo | Frente | Fonte | Extensao | Quantidade |
|---|---|---|---|---:|
| raw | nao_classificado | nao_identificada | .csv | 849 |
| raw | nao_classificado | nao_identificada | .txt | 487 |
| raw | rural | nao_identificada | .txt | 252 |
| raw | far | nao_identificada | .txt | 241 |
| raw | fgts_financiado | nao_identificada | .mdb | 140 |
| raw | entidades | nao_identificada | .txt | 134 |
| raw | fgts_financiado | fgts | .txt | 132 |
| raw | nao_classificado | entregas | .csv | 95 |
| raw | nao_classificado | nao_identificada | .xlsx | 92 |
| raw | far | nao_identificada | .csv | 74 |
| raw | fgts_financiado | fgts | .csv | 53 |
| raw | fgts_financiado | nao_identificada | .xlsx | 35 |
| raw | rural | nao_identificada | .csv | 34 |
| raw | nao_classificado | nao_identificada | .xls | 29 |
| raw | entidades | nao_identificada | .xls | 22 |
| raw | conjuntura | nao_identificada | .json | 17 |
| raw | nao_classificado | entregas | .txt | 17 |
| raw | entidades | nao_identificada | .csv | 14 |
| raw | mcmv_geral | nao_identificada | .csv | 8 |
| raw | conjuntura | nao_identificada | .csv | 5 |
| raw | nao_classificado | nao_identificada |  | 4 |
| raw | nao_classificado | nao_identificada | .json | 4 |
| raw | entidades | nao_identificada | .xlsx | 3 |
| raw | far | nao_identificada | .xlsx | 3 |
| raw | conjuntura | nao_identificada | .xlsx | 2 |
| raw | entidades | fgts | .csv | 2 |
| raw | far | entregas | .csv | 2 |
| raw | ogu_subsidiado | ogu | .csv | 2 |
| raw | sub50_fnhis | nao_identificada | .csv | 2 |
| raw | conjuntura | nao_identificada |  | 1 |
| raw | entidades | fgts | .txt | 1 |
| raw | fgts_financiado | nao_identificada | .geavo | 1 |
| raw | fgts_financiado | nao_identificada | .json | 1 |
| raw | mcmv_geral | entregas | .csv | 1 |
| raw | mcmv_geral | nao_identificada | .xlsx | 1 |
| raw | nao_classificado | entregas | .xlsx | 1 |
| raw | nao_classificado | nao_identificada | .zip | 1 |

## Evidencias geradas

- `minio_inventory_prefixos_consultados.csv`: inventario dos objetos MinIO nos prefixos consultados.
- `sharepoint_vs_minio_diff.csv`: diff arquivo a arquivo do SharePoint local contra MinIO.
- `sharepoint_vs_minio_resumo_por_frente.csv`: resumo do diff por frente.
- `minio_only_sem_match_sharepoint.csv`: objetos no MinIO sem nome/stem equivalente no SharePoint local.
- `minio_resumo_por_prefixo_frente.csv`: resumo do MinIO por prefixo/frente/fonte/extensao.
