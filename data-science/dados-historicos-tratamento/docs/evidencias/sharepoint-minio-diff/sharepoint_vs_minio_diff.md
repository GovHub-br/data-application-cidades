# Diff SharePoint local x MinIO

- Gerado em: `2026-08-19 02:28:02 UTC`
- Prefixos MinIO consultados: `staging/`
- Arquivos candidatos no SharePoint local: `1400`
- Objetos MinIO inventariados: `0`
- Match forte por nome/stem: `0`
- Pista semantica sem match de nome: `0`
- Nao encontrados no MinIO consultado: `1400`
- Objetos MinIO sem match no SharePoint: `0`

## Como interpretar

- `existe_nome_exato`: arquivo do SharePoint aparece no MinIO com o mesmo nome.
- `existe_mesmo_stem_extensao_diferente`: parece o mesmo artefato convertido, por exemplo CSV/TXT no SharePoint e Parquet no MinIO.
- `pista_semantica_mesma_frente_periodo_fonte`: existe algo no MinIO com mesma frente/periodo/fonte, mas o nome nao comprova equivalencia.
- `nao_encontrado_no_minio_consultado`: o SharePoint complementa o MinIO nos prefixos consultados, ou o arquivo foi renomeado fora das regras de match.

## Resultado por frente

| Frente | Match | Quantidade |
|---|---|---:|
| entidades | nao_encontrado_no_minio_consultado | 355 |
| far | nao_encontrado_no_minio_consultado | 698 |
| fgts_entregas | nao_encontrado_no_minio_consultado | 13 |
| fgts_financiado | nao_encontrado_no_minio_consultado | 140 |
| nao_classificado | nao_encontrado_no_minio_consultado | 27 |
| ogu_subsidiado | nao_encontrado_no_minio_consultado | 43 |
| rural | nao_encontrado_no_minio_consultado | 124 |

## MinIO por prefixo/frente

| Prefixo | Frente | Fonte | Extensao | Quantidade |
|---|---|---|---|---:|

## Evidencias geradas

- `minio_inventory_prefixos_consultados.csv`: inventario dos objetos MinIO nos prefixos consultados.
- `sharepoint_vs_minio_diff.csv`: diff arquivo a arquivo do SharePoint local contra MinIO.
- `sharepoint_vs_minio_resumo_por_frente.csv`: resumo do diff por frente.
- `minio_only_sem_match_sharepoint.csv`: objetos no MinIO sem nome/stem equivalente no SharePoint local.
- `minio_resumo_por_prefixo_frente.csv`: resumo do MinIO por prefixo/frente/fonte/extensao.
