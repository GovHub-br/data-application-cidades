# Auditoria dos dashboards Superset MCMV x SharePoint x MinIO

- Gerado em: `2026-08-19 03:19:38 UTC`
- Escopo: dashboards `FAR`, `RURAL` e `Entidades`.
- Dashboards auditados: `3`
- Charts auditados: `85`
- Datasets distintos usados: `12`

## Leitura executiva

- Os 12 datasets que alimentam FAR, RURAL e Entidades existem no PostgreSQL e foram perfilados com contagem de linhas.
- Por frente, `12/12` datasets tem fontes candidatas no SharePoint local e `12/12` tem fontes candidatas no bucket MinIO.
- Em `raw/sharepoint/`, `12/12` datasets tem fonte candidata; portanto os dados do SharePoint ja estao no MinIO no caminho confirmado.
- A busca por valores reais encontrou amostras do Superset em `9/12` datasets dentro dos arquivos SharePoint baixados.
- Match direto por nome de tabela e esperado dar falso negativo: os dashboards usam tabelas analiticas/agregadas, enquanto as fontes usam nomes operacionais como `MONIT_CAD_PJ_*`, `MONIT_MOV_FINANC_*`, `HIS_MCIDADES_CONSOLIDADO_*`.
- Foi identificado um alerta de modelagem de dashboard: `Entidades` usa um dataset da frente `FAR` no chart `Estado`.

## Charts por dashboard

| Dashboard | Frente | Charts |
|---|---|---:|
| Entidades | entidades | 34 |
| FAR | far | 24 |
| RURAL | rural | 27 |

## Datasets que alimentam os dashboards

| Dashboard | Dataset ID | Schema | Tabela | Frente dataset | Charts | Linhas Postgres |
|---|---:|---|---|---|---:|---:|
| Entidades | 34 | empreendimento_far | panorama_estadual | far | 1 | 1366 |
| Entidades | 82 | entidades_fds | fds_ficha_empreendimento | entidades | 31 | 343 |
| Entidades | 84 | entidades_fds | fds_evolucao_financeira_chart | entidades | 2 | 306 |
| FAR | 32 | empreendimento_far | ficha_empreendimento | far | 10 | 1646 |
| FAR | 34 | empreendimento_far | panorama_estadual | far | 11 | 1366 |
| FAR | 35 | empreendimento_far | mapa_nacional | far | 2 | 32 |
| FAR | 80 | empreendimento_far | execucao_fisica_financeira_chart | far | 1 | 17366 |
| RURAL | 86 | empreendimento_rural | ficha_empreendimento_rural | rural | 14 | 10402 |
| RURAL | 87 | empreendimento_rural | panorama_estadual_rural | rural | 8 | 326 |
| RURAL | 88 | empreendimento_rural | execucao_fisica_financeira_chart_rural | rural | 1 | 59004 |
| RURAL | 89 | empreendimento_rural | mapa_nacional_rural | rural | 2 | 32 |
| RURAL | 90 | empreendimento_rural | perfil_beneficiarios | rural | 1 | 103 |
| RURAL | 91 | empreendimento_rural | infraestrutura_agua_saneamento | rural | 1 | 10402 |

## Alertas de dashboard usando dataset de outra frente

| Dashboard | Frente dashboard | Dataset | Frente dataset | Charts |
|---|---|---|---|---:|
| Entidades | entidades | empreendimento_far.panorama_estadual | far | 1 |

## Existencia por dataset

| Dataset | SharePoint nome | SharePoint frente | MinIO bucket nome | MinIO bucket frente | MinIO raw/sharepoint nome | MinIO raw/sharepoint frente |
|---|---|---|---|---|---|---|
| entidades_fds.fds_evolucao_financeira_chart | False | True | False | True | False | True |
| entidades_fds.fds_ficha_empreendimento | False | True | False | True | False | True |
| empreendimento_far.execucao_fisica_financeira_chart | False | True | False | True | False | True |
| empreendimento_far.ficha_empreendimento | False | True | False | True | False | True |
| empreendimento_far.mapa_nacional | False | True | False | True | False | True |
| empreendimento_far.panorama_estadual | False | True | False | True | False | True |
| empreendimento_rural.execucao_fisica_financeira_chart_rural | False | True | False | True | False | True |
| empreendimento_rural.ficha_empreendimento_rural | False | True | False | True | False | True |
| empreendimento_rural.infraestrutura_agua_saneamento | False | True | False | True | False | True |
| empreendimento_rural.mapa_nacional_rural | False | True | False | True | False | True |
| empreendimento_rural.panorama_estadual_rural | False | True | False | True | False | True |
| empreendimento_rural.perfil_beneficiarios | False | True | False | True | False | True |

## Exemplos de fontes candidatas

### entidades_fds.fds_evolucao_financeira_chart
- SharePoint: OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/MONIT_MOV_OBRA_FDS_SEMANAL_202606_042914.CSV | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FDS_20251212_103018.CSV | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FDS_20251215_103029.CSV | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FDS_20251216_103023.CSV | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FDS_20251217_103015.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_fds_financeiro_mensal.csv | raw/sharepoint/novo_mcmv_fds_obra_diario.csv | raw/sharepoint/novo_mcmv_fds_financeiro_semanal.csv | raw/sharepoint/novo_mcmv_fds_obra_mensal.csv | raw/sharepoint/novo_mcmv_fds_obra_semanal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_fds_obra_mensal.csv | raw/sharepoint/novo_mcmv_fds_obra_diario.csv | raw/sharepoint/novo_mcmv_fds_financeiro_semanal.csv | raw/sharepoint/novo_mcmv_fds_obra_semanal.csv | raw/sharepoint/novo_mcmv_fds_financeiro_mensal.csv
### entidades_fds.fds_ficha_empreendimento
- SharePoint: OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FDS_20251126_041015.csv | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FDS_20251127_041040.csv | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FDS_20251128_041040.csv | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FDS_20251201_041012.csv | OneDrive_2026-08-19 (4).zip::Novo MCMV - FDS/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FDS_20251202_041000.csv
- MinIO bucket: raw/sharepoint/novo_mcmv_fds_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_fds_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_fds_obra_mensal.csv | raw/sharepoint/novo_mcmv_fds_obra_diario.csv | raw/sharepoint/novo_mcmv_fds_financeiro_semanal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_fds_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_fds_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_fds_trabalho_social_diario.csv | raw/sharepoint/novo_mcmv_fds_obra_semanal.csv | raw/sharepoint/novo_mcmv_fds_obra_mensal.csv
### empreendimento_far.execucao_fisica_financeira_chart
- SharePoint: OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/MONIT_MOV_OBRA_FAR_SEMANAL_202604_043306.CSV_Error.txt | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/MONIT_MOV_OBRA_FAR_SEMANAL_202604_043611.CSV_Error.txt | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/MONIT_MOV_OBRA_FAR_SEMANAL_202604_043742.CSV_Error.txt | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/MONIT_MOV_OBRA_FAR_SEMANAL_202604_043812.CSV_Error.txt | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/MONIT_MOV_OBRA_FAR_SEMANAL_202605_042749.CSV_Error.txt
- MinIO bucket: raw/sharepoint/novo_mcmv_far_financeiro_diario.csv | raw/sharepoint/novo_mcmv_far_financeiro_mensal.csv | raw/sharepoint/novo_mcmv_far_obra_mensal.csv | raw/sharepoint/novo_mcmv_far_obra_semanal.csv | raw/sharepoint/novo_mcmv_far_financeiro_semanal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_far_financeiro_diario.csv | raw/sharepoint/novo_mcmv_far_financeiro_mensal.csv | raw/sharepoint/novo_mcmv_far_financeiro_semanal.csv | raw/sharepoint/novo_mcmv_far_obra_diario.csv | raw/sharepoint/novo_mcmv_far_obra_semanal.csv
### empreendimento_far.ficha_empreendimento
- SharePoint: OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250324.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250324.txt | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250401.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250401.txt | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250407.csv
- MinIO bucket: raw/sharepoint/novo_mcmv_far_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_far_consolidado.csv | raw/sharepoint/novo_mcmv_far_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_far_contratacao.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_far_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_far_consolidado.csv | raw/sharepoint/novo_mcmv_far_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_far_contratacao.csv
### empreendimento_far.mapa_nacional
- SharePoint: OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250324.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250324.txt | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250401.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250401.txt | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250407.csv
- MinIO bucket: raw/sharepoint/novo_mcmv_far_consolidado.csv | raw/sharepoint/novo_mcmv_far_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_far_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_far_contratacao.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_far_consolidado.csv | raw/sharepoint/novo_mcmv_far_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_far_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_far_contratacao.csv
### empreendimento_far.panorama_estadual
- SharePoint: OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250324.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250324.txt | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250401.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250401.txt | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250407.csv
- MinIO bucket: raw/sharepoint/novo_mcmv_far_consolidado.csv | raw/sharepoint/novo_mcmv_far_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_far_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_far_contratacao.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_far_consolidado.csv | raw/sharepoint/novo_mcmv_far_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_far_cad_pj_semanal.csv | raw/sharepoint/novo_mcmv_far_contratacao.csv
### empreendimento_rural.execucao_fisica_financeira_chart_rural
- SharePoint: OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_RURAL_20251223_124903.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_RURAL_DIARIO_20251230_103055.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_RURAL_DIARIO_20251229_103027.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_RURAL_20251224_103041.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_RURAL_DIARIO_20251231_103040.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv
### empreendimento_rural.ficha_empreendimento_rural
- SharePoint: OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_RURAL_20251224_101226.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202512_041043.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202601_042245.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_MENSAL_202604_043834.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_LAYOUT_20260401_043425.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv
### empreendimento_rural.infraestrutura_agua_saneamento
- SharePoint: OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_RURAL_20251224_101226.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202512_041043.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202601_042245.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_MENSAL_202604_043834.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_LAYOUT_20260401_043425.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_mensal.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_diario.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_mensal.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_diario.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv
### empreendimento_rural.mapa_nacional_rural
- SharePoint: OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_RURAL_20251224_101226.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202512_041043.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202601_042245.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_MENSAL_202604_043834.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_LAYOUT_20260401_043425.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv
### empreendimento_rural.panorama_estadual_rural
- SharePoint: OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_RURAL_20251224_101226.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202512_041043.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202601_042245.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_MENSAL_202604_043834.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_LAYOUT_20260401_043425.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_obra_semanal.csv | raw/sharepoint/novo_mcmv_rural_obra_mensal.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv
### empreendimento_rural.perfil_beneficiarios
- SharePoint: OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_RURAL_20251224_101226.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202512_041043.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/RURAL - Cadastro PJ - Mensal/MONIT_CAD_PJ_RURAL_MENSAL_202601_042245.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_MENSAL_202604_043834.CSV | OneDrive_2026-08-19 (2).zip::Novo MCMV - Rural/MONIT_CAD_PJ_RURAL_LAYOUT_20260401_043425.CSV
- MinIO bucket: raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_mensal.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_diario.csv | raw/sharepoint/novo_mcmv_rural_financeiro_mensal.csv
- MinIO raw/sharepoint: raw/sharepoint/novo_mcmv_rural_cad_pj_diario.csv | raw/sharepoint/novo_mcmv_rural_cad_pj_mensal.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_mensal.csv | raw/sharepoint/novo_mcmv_rural_cadastro_pf_diario.csv | raw/sharepoint/novo_mcmv_rural_financeiro_semanal.csv

## Datasets sem evidência em SharePoint nem MinIO

Nenhum dataset dos tres dashboards ficou totalmente sem evidencia semantica em SharePoint/MinIO. O ponto de atencao e que a equivalencia aparece por frente/familia de arquivos, nao por nome identico de tabela analitica.

## Evidencia por conteudo amostrado no SharePoint local

- `empreendimento_far.execucao_fisica_financeira_chart`: encontrou amostra no SharePoint.
- `empreendimento_far.ficha_empreendimento`: encontrou amostra no SharePoint.
- `empreendimento_far.mapa_nacional`: encontrou amostra no SharePoint.
- `empreendimento_far.panorama_estadual`: encontrou amostra no SharePoint.
- `empreendimento_rural.execucao_fisica_financeira_chart_rural`: nao encontrou amostra nos arquivos CSV/TXT/XLSX locais.
- `empreendimento_rural.ficha_empreendimento_rural`: nao encontrou amostra nos arquivos CSV/TXT/XLSX locais.
- `empreendimento_rural.infraestrutura_agua_saneamento`: nao encontrou amostra nos arquivos CSV/TXT/XLSX locais.
- `empreendimento_rural.mapa_nacional_rural`: encontrou amostra no SharePoint.
- `empreendimento_rural.panorama_estadual_rural`: encontrou amostra no SharePoint.
- `empreendimento_rural.perfil_beneficiarios`: encontrou amostra no SharePoint.
- `entidades_fds.fds_evolucao_financeira_chart`: encontrou amostra no SharePoint.
- `entidades_fds.fds_ficha_empreendimento`: encontrou amostra no SharePoint.

## Evidencia por conteudo amostrado no MinIO

Nao houve varredura de conteudo no MinIO.

## Arquivos gerados

- `superset_dashboards_alvo.csv`
- `superset_dashboard_charts_alvo.csv`
- `superset_dashboard_datasets_alvo.csv`
- `superset_dashboard_dataset_sources.csv`
- `superset_dashboard_table_profiles.csv`
- `superset_dashboard_table_samples.csv`
- `superset_dashboard_sample_sharepoint_hits.csv`
- `superset_dashboard_sample_minio_name_hits.csv`
- `superset_dashboard_sample_minio_content_hits.csv`
