# Relatorio 02 - MinIO raw e reorganizacao SFTP/SharePoint

- Gerado em: `2026-08-19 03:25:03 UTC`
- Bucket analisado: `data-lake-mcid`.
- Prefixos relidos nesta rodada: `raw/` e `raw/sharepoint/`.
- Objetos em `raw/`: `2762`
- Objetos em `raw/sharepoint/`: `138`

## Distribuicao no raw

| Area | Frente | Qtd |
| --- | --- | --- |
| sftp | nao_classificado | 797 |
| dados_historicos | nao_classificado | 720 |
| sftp | fgts_financiado | 307 |
| sftp | far | 295 |
| sftp | rural | 252 |
| sftp | entidades | 160 |
| sharepoint | fgts_financiado | 51 |
| ementario | nao_classificado | 29 |
| sharepoint | nao_classificado | 27 |
| dados_historicos | rural | 20 |
| sharepoint | far | 18 |
| sharepoint | entidades | 16 |
| sharepoint | rural | 14 |
| ibge | conjuntura | 14 |
| dados_historicos | far | 7 |
| dados_historicos | mcmv_geral | 5 |
| sharepoint | mcmv_geral | 4 |
| sharepoint | conjuntura | 4 |
| fgv | conjuntura | 4 |
| bacen | nao_classificado | 2 |
| dados_historicos | fgts_financiado | 2 |
| abecip | fgts_financiado | 2 |
| sharepoint | sub50_fnhis | 2 |
| sharepoint | ogu_subsidiado | 2 |
| siafi-tesouro-gerencial | nao_classificado | 2 |
| fipe | conjuntura | 2 |
| novo_caged | conjuntura | 1 |
| infomoney | nao_classificado | 1 |
| construtoras | nao_classificado | 1 |
| sftp | mcmv_geral | 1 |

## Distribuicao em raw/sharepoint

| Frente | Extensao | Qtd |
| --- | --- | --- |
| fgts_financiado | .csv | 51 |
| nao_classificado | .csv | 26 |
| far | .csv | 18 |
| entidades | .csv | 16 |
| rural | .csv | 14 |
| conjuntura | .csv | 4 |
| mcmv_geral | .csv | 4 |
| ogu_subsidiado | .csv | 2 |
| sub50_fnhis | .csv | 2 |
| nao_classificado |  | 1 |

## Diff SharePoint local x MinIO raw

| Tipo de match | Qtd |
| --- | --- |
| pista_semantica_mesma_frente_periodo_fonte | 937 |
| nao_encontrado_no_minio_consultado | 463 |

## Diff SharePoint local x MinIO raw/sharepoint

| Tipo de match | Qtd |
| --- | --- |
| nao_encontrado_no_minio_consultado | 1400 |

## Leitura

- Nao ha indicio de perda dos dados SFTP: `raw/sftp/` segue presente e concentra a maior massa por FAR, Rural, Entidades e FGTS.
- O que mudou foi organizacao/publicacao: parte do acervo SharePoint aparece como CSVs consolidados em `raw/sharepoint/`.
- O diff por nome exato entre SharePoint local e `raw/sharepoint/` nao bate porque os objetos do MinIO foram renomeados para nomes canonicos, como `novo_mcmv_far_consolidado.csv`.
- Para dbt/silver, o caminho pratico e consumir `raw/sharepoint/` e manter `raw/sftp/` como fonte bruta historica/auditavel.
