# Inventario local SharePoint - MCMV

- Gerado em: `2026-08-30 16:22:58 UTC`
- Pasta inventariada: `/home/juan-pablo/CIDADES/sharepoint`
- Itens no filesystem: `2443`
- Itens dentro de ZIPs: `474`
- Candidatos a dado/documento/dashboard: `2415`

## Leitura executiva

- O pacote local baixado combina bases Casa Civil do MCMV OGU/FGTS, pacotes de dados do Novo MCMV e materiais de catalogo/metadados.
- Os nomes indicam bases mensais de contratacoes/dados OGU, contratacoes/dados FGTS, entregas FGTS e arquivos especificos de FAR/Entidades/Rural.
- Ha fichas de metadados em XLSX para OGU, FGTS e entregas FGTS, que devem orientar o mapeamento semantico antes de transformar em silver.
- A inferencia por nome/caminho identifica a fonte macro, mas nao substitui leitura de colunas para separar FAR, Entidades, Rural, SUB50/FNHIS e demais modalidades quando elas estiverem dentro de arquivos OGU.

## Resumo por frente primaria

| Frente primaria | Fonte | Categoria | Quantidade |
|---|---|---|---:|
| far | nao_identificada | dado_tabular_ou_serializado | 790 |
| entidades | nao_identificada | dado_tabular_ou_serializado | 378 |
| nao_classificado | nao_identificada | dado_tabular_ou_serializado | 242 |
| fgts_financiado | fgts | dado_tabular_ou_serializado | 214 |
| sub50_fnhis | nao_identificada | dado_tabular_ou_serializado | 174 |
| rural | nao_identificada | dado_tabular_ou_serializado | 148 |
| conjuntura | nao_identificada | dado_tabular_ou_serializado | 143 |
| fgts_financiado | fgts | arquivo_compactado | 109 |
| sub50_fnhis | nao_identificada | arquivo_compactado | 74 |
| mcmv_geral | nao_identificada | documento | 52 |
| mcmv_geral | nao_identificada | dado_tabular_ou_serializado | 51 |
| ogu_subsidiado | ogu | dado_tabular_ou_serializado | 15 |
| nao_classificado | nao_identificada | documento | 8 |
| nao_classificado | nao_identificada | arquivo_compactado | 6 |
| entidades | metadados | documento | 2 |
| far | metadados | documento | 2 |
| far | nao_identificada | arquivo_compactado | 2 |
| metadados | metadados | documento | 2 |
| nao_classificado | entregas | dado_tabular_ou_serializado | 2 |
| entidades | nao_identificada | arquivo_compactado | 1 |

## Resumo por todas as tags de frente/fonte inferidas

| Frente | Fonte | Categoria | Quantidade |
|---|---|---|---:|
| mcmv_geral | nao_identificada | dado_tabular_ou_serializado | 1364 |
| far | nao_identificada | dado_tabular_ou_serializado | 790 |
| entidades | nao_identificada | dado_tabular_ou_serializado | 394 |
| nao_classificado | nao_identificada | dado_tabular_ou_serializado | 242 |
| fgts_financiado | fgts | dado_tabular_ou_serializado | 214 |
| sub50_fnhis | nao_identificada | dado_tabular_ou_serializado | 174 |
| rural | nao_identificada | dado_tabular_ou_serializado | 155 |
| conjuntura | nao_identificada | dado_tabular_ou_serializado | 143 |
| fgts_financiado | fgts | arquivo_compactado | 109 |
| sub50_fnhis | nao_identificada | arquivo_compactado | 74 |
| mcmv_geral | fgts | dado_tabular_ou_serializado | 62 |
| mcmv_geral | nao_identificada | documento | 52 |
| ogu_subsidiado | ogu | dado_tabular_ou_serializado | 15 |
| nao_classificado | nao_identificada | documento | 8 |
| metadados | metadados | documento | 6 |
| nao_classificado | nao_identificada | arquivo_compactado | 6 |
| mcmv_geral | metadados | documento | 4 |
| mcmv_geral | nao_identificada | arquivo_compactado | 3 |
| entidades | metadados | documento | 2 |
| far | metadados | documento | 2 |
| far | nao_identificada | arquivo_compactado | 2 |
| nao_classificado | entregas | dado_tabular_ou_serializado | 2 |
| entidades | nao_identificada | arquivo_compactado | 1 |

## Resumo por extensao

| Extensao | Categoria | Quantidade |
|---|---|---:|
| .csv | dado_tabular_ou_serializado | 1464 |
| .xlsx | dado_tabular_ou_serializado | 342 |
| .txt | dado_tabular_ou_serializado | 209 |
| .zip | arquivo_compactado | 192 |
| .xls | dado_tabular_ou_serializado | 142 |
| .pptx | documento | 52 |
| .pdf | documento | 14 |

## Resumo por periodo/fonte

| Periodo | Fonte | Quantidade |
|---|---|---:|
| 2009-02 | metadados | 4 |
| 2009-02 | nao_identificada | 6 |
| 2012-02 | nao_identificada | 2 |
| 2020-01 | fgts | 1 |
| 2020-02 | fgts | 1 |
| 2021-01 | fgts | 1 |
| 2021-02 | fgts | 1 |
| 2022-01 | fgts | 1 |
| 2022-02 | fgts | 1 |
| 2022-02 | nao_identificada | 2 |
| 2023-01 | fgts | 1 |
| 2023-02 | fgts | 1 |
| 2023-02 | nao_identificada | 7 |
| 2024-01 | fgts | 1 |
| 2024-02 | fgts | 1 |
| 2024-02 | nao_identificada | 4 |
| 2024-06 | ogu | 1 |
| 2024-07 | nao_identificada | 4 |
| 2024-07 | ogu | 1 |
| 2024-08 | ogu | 1 |
| 2024-09 | ogu | 1 |
| 2024-10 | fgts | 1 |
| 2024-10 | nao_identificada | 1 |
| 2024-10 | ogu | 1 |
| 2024-11 | nao_identificada | 5 |
| 2024-11 | ogu | 1 |
| 2024-12 | fgts | 8 |
| 2024-12 | ogu | 2 |
| 2025-01 | fgts | 9 |
| 2025-01 | ogu | 1 |
| 2025-02 | fgts | 12 |
| 2025-03 | fgts | 7 |
| 2025-03 | nao_identificada | 9 |
| 2025-03 | ogu | 2 |
| 2025-04 | fgts | 8 |
| 2025-04 | nao_identificada | 12 |
| 2025-05 | fgts | 9 |
| 2025-05 | nao_identificada | 25 |
| 2025-06 | fgts | 7 |
| 2025-06 | nao_identificada | 25 |
| 2025-06 | ogu | 1 |
| 2025-07 | fgts | 8 |
| 2025-07 | nao_identificada | 31 |
| 2025-08 | entregas | 2 |
| 2025-08 | fgts | 10 |
| 2025-08 | nao_identificada | 47 |
| 2025-08 | ogu | 1 |
| 2025-09 | fgts | 25 |
| 2025-09 | nao_identificada | 84 |
| 2025-09 | ogu | 1 |
| 2025-10 | fgts | 27 |
| 2025-10 | nao_identificada | 135 |
| 2025-11 | fgts | 23 |
| 2025-11 | nao_identificada | 138 |
| 2025-12 | fgts | 22 |
| 2025-12 | nao_identificada | 169 |
| 2026-01 | fgts | 22 |
| 2026-01 | nao_identificada | 238 |
| 2026-02 | fgts | 21 |
| 2026-02 | nao_identificada | 180 |
| 2026-02 | ogu | 1 |
| 2026-03 | fgts | 24 |
| 2026-03 | nao_identificada | 117 |
| 2026-04 | fgts | 23 |
| 2026-04 | nao_identificada | 121 |
| 2026-05 | fgts | 18 |
| 2026-05 | nao_identificada | 125 |
| 2026-06 | fgts | 1 |
| 2026-06 | nao_identificada | 120 |
| 2026-07 | nao_identificada | 110 |
| 2026-08 | nao_identificada | 84 |
| 2032-02 | nao_identificada | 2 |
| 2062-02 | nao_identificada | 2 |
| 2092-02 | nao_identificada | 2 |

## Principais pacotes

| Pacote | Frente primaria | Fonte | Quantidade |
|---|---|---|---:|
| filesystem | far | nao_identificada | 785 |
| filesystem | entidades | nao_identificada | 374 |
| filesystem | fgts_financiado | fgts | 323 |
| filesystem | nao_classificado | nao_identificada | 254 |
| filesystem | sub50_fnhis | nao_identificada | 177 |
| filesystem | rural | nao_identificada | 144 |
| filesystem | conjuntura | nao_identificada | 143 |
| filesystem | mcmv_geral | nao_identificada | 103 |
| filesystem | ogu_subsidiado | ogu | 15 |
| Novo MCMV - FAR/FAR - Consolidado/anexos_mcid.zip | far | nao_identificada | 2 |
| filesystem | nao_classificado | entregas | 2 |
| Geohabitação/Bases do Gabinete - Adam/MCMV_FAR_BR_2009_2022.zip | far | metadados | 1 |
| Geohabitação/Bases do Gabinete - Adam/MCMV_FAR_BR_2009_2022.zip | far | nao_identificada | 1 |
| Geohabitação/Bases do Gabinete - Adam/MCMV_FDS_BR_2009_2022.zip | entidades | metadados | 1 |
| Geohabitação/Bases do Gabinete - Adam/MCMV_FDS_BR_2009_2022.zip | entidades | nao_identificada | 1 |
| Geohabitação/Bases do Gabinete - Adam/PAR_BRASIL_POLIGONAIS.zip | metadados | metadados | 1 |
| Geohabitação/Bases do Gabinete - Adam/PAR_BRASIL_POLIGONAIS.zip | nao_classificado | nao_identificada | 1 |
| Oferta Pública/Arquivados/36. BASE OFERTA - CONSOLIDADA EM 30.01.2026 - Com Banco Morada.zip | nao_classificado | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_03-06-2025 G.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_03-06-2025 M.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_04-08-2025 G.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_07-04-2025.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_09-06-2025 G.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_09-06-2025 M.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_12-05-2025 G.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_12-05-2025 M.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_14-04-2025.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_16-06-2025 - M.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_16-06-2025 G.zip | sub50_fnhis | nao_identificada | 1 |
| SNHIS/Arquivados/FNHIS_SEMANAL_19-05-2025 G.zip | sub50_fnhis | nao_identificada | 1 |

## Evidencias CSV

- `sharepoint_local_arquivos.csv`: arquivos e pastas locais baixados.
- `sharepoint_local_zip_conteudo.csv`: conteudo interno dos arquivos ZIP.
- `sharepoint_local_candidatos_mcmv.csv`: arquivos com extensoes relevantes para dados, dashboards ou documentacao.
- `sharepoint_local_resumo_por_frente_primaria.csv`: contagem pela frente mais especifica inferida.
- `sharepoint_local_resumo_por_frente.csv`: contagem por frente/fonte/categoria inferida.
- `sharepoint_local_resumo_por_extensao.csv`: contagem por extensao.
- `sharepoint_local_resumo_por_periodo.csv`: contagem por periodo mensal inferido.
- `sharepoint_local_resumo_por_pacote.csv`: contagem por ZIP/pacote de origem.

## Proximo uso na arquitetura

Para respeitar a arquitetura definida, estes arquivos devem ser tratados como fonte de inventario/descoberta. A carga silver produtiva deve consumir apenas arquivos publicados na camada `staging/` do MinIO via DuckDB/dbt.
