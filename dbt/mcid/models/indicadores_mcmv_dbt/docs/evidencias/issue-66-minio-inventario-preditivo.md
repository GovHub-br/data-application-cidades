# Evidencia MinIO Issue 66 - Inventario Preditivo

- Endpoint: `10.0.0.56:9000` via `http`
- Bucket: `data-lake-mcid`
- Objetos listados: `6697`
- Tamanho total aproximado: `297518.0 MB`

## Objetos Por Camada
| layer | objetos | tamanho_mb |
| --- | --- | --- |
| staging | 3958 | 30133.6 |
| raw | 2703 | 267381.3 |
| audit | 20 | 0.5 |
| staging_dryrun | 14 | 2.6 |
| test | 2 | 0.0 |

## Objetos Por Extensao
| extension | objetos | tamanho_mb |
| --- | --- | --- |
| parquet | 3993 | 30136.8 |
| txt | 1260 | 135804.4 |
| csv | 1137 | 9304.3 |
| mdb | 140 | 118550.3 |
| xlsx | 135 | 71.7 |
| xls | 31 | 2.5 |
| zip | 1 | 3648.0 |

## Tags Preditivas Detectadas
| tag | objetos | tamanho_mb |
| --- | --- | --- |
| responsavel | 4244 | 33956.5 |
| execucao_fisica | 1134 | 4147.6 |
| far | 640 | 21643.7 |
| prioritarios_snh | 619 | 655.0 |
| fgts_sbpe | 469 | 79258.7 |
| fds_entidades | 466 | 849.4 |
| entrega_uh | 278 | 79.5 |
| financeiro_desembolso | 242 | 1308.2 |
| contratacao_uh | 211 | 308.8 |
| rural_fnhis | 121 | 9.0 |
| territorio | 111 | 10.7 |
| prazo_atraso | 41 | 2.1 |
| validacao_qualidade | 27 | 270.1 |

## Top Arquivos Candidatos Para Indicadores/Alertas
| key | layer | extension | size_mb | year_hint | tags_preditive | score_preditivo |
| --- | --- | --- | --- | --- | --- | --- |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251024.csv | raw | csv | 0.055 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250919.csv | raw | csv | 0.037 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251024.csv.parquet | staging | parquet | 0.024 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250912.csv | raw | csv | 0.02 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251114.csv | raw | csv | 0.02 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250919.csv.parquet | staging | parquet | 0.019 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251003.csv | raw | csv | 0.019 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251107.csv | raw | csv | 0.018 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251031.csv | raw | csv | 0.018 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250730.xlsx | raw | xlsx | 0.017 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250730.xlsx.parquet | staging | parquet | 0.016 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250912.csv.parquet | staging | parquet | 0.014 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251114.csv.parquet | staging | parquet | 0.014 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251003.csv.parquet | staging | parquet | 0.014 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251017.csv | raw | csv | 0.013 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251107.csv.parquet | staging | parquet | 0.013 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251031.csv.parquet | staging | parquet | 0.013 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251017.csv.parquet | staging | parquet | 0.011 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250926.csv | raw | csv | 0.011 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250926.csv.parquet | staging | parquet | 0.011 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250905.csv.parquet | staging | parquet | 0.01 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250829.csv.parquet | staging | parquet | 0.01 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250905.csv | raw | csv | 0.009 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| raw/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250829.csv | raw | csv | 0.009 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251010.csv.parquet | staging | parquet | 0.008 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250822.csv.parquet | staging | parquet | 0.008 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260130.csv.parquet | staging | parquet | 0.008 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260123.csv.parquet | staging | parquet | 0.008 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260116.csv.parquet | staging | parquet | 0.008 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260109.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260327.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260529.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260410.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251226.csv.parquet | staging | parquet | 0.007 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20250815.csv.parquet | staging | parquet | 0.007 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260417.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251121.csv.parquet | staging | parquet | 0.007 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260505.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260508.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260102.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260313.csv.parquet | staging | parquet | 0.007 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251205.csv.parquet | staging | parquet | 0.006 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260403.csv.parquet | staging | parquet | 0.006 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260515.csv.parquet | staging | parquet | 0.006 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260306.csv.parquet | staging | parquet | 0.006 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260213.csv.parquet | staging | parquet | 0.006 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251128.csv.parquet | staging | parquet | 0.006 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260703.csv.parquet | staging | parquet | 0.006 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20260612.csv.parquet | staging | parquet | 0.006 | 2026.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
| staging/Dados_Prioritarios_Contratacoes_MCMV_FAR_FDS_RURAL_Semanal_20251212.csv.parquet | staging | parquet | 0.006 | 2025.0 | contratacao_uh,far,fds_entidades,rural_fnhis,prioritarios_snh | 9 |
