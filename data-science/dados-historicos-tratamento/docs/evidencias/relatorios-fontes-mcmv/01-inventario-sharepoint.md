# Relatorio 01 - Inventario SharePoint MCMV

- Gerado em: `2026-08-19 03:25:03 UTC`
- Pasta local inventariada: `/home/juan-pablo/CIDADES/sharepoint`
- Arquivos/pastas no filesystem: `55`
- Itens encontrados dentro de ZIPs: `1366`
- Candidatos MCMV inventariados: `1400`

## Resumo por frente

| Frente | Categoria | Qtd |
| --- | --- | --- |
| entidades | dado_tabular_ou_serializado | 354 |
| entidades | documento | 1 |
| far | dado_tabular_ou_serializado | 696 |
| far | documento | 2 |
| fgts_entregas | dado_tabular_ou_serializado | 7 |
| fgts_entregas | arquivo_compactado | 6 |
| fgts_financiado | dado_tabular_ou_serializado | 132 |
| fgts_financiado | arquivo_compactado | 8 |
| nao_classificado | documento | 16 |
| nao_classificado | arquivo_compactado | 9 |
| nao_classificado | dado_tabular_ou_serializado | 2 |
| ogu_subsidiado | arquivo_compactado | 22 |
| ogu_subsidiado | dado_tabular_ou_serializado | 21 |
| rural | dado_tabular_ou_serializado | 123 |
| rural | documento | 1 |

## Janela temporal inferida

| Frente | Inicio | Fim | Arquivos |
| --- | --- | --- | --- |
| entidades | 2025-10 | 2026-08 | 354 |
| far | 2025-03 | 2026-08 | 690 |
| fgts_entregas | 2023-08 | 2024-09 | 13 |
| fgts_financiado | 2023-08 | 2026-05 | 140 |
| nao_classificado | 2025-05 | 2025-05 | 1 |
| ogu_subsidiado | 2023-05 | 2024-09 | 43 |
| rural | 2024-02 | 2026-08 | 123 |

## Extensoes principais

| Extensao | Qtd |
| --- | --- |
| .csv | 678 |
| .txt | 622 |
| .zip | 45 |
| .xlsx | 35 |
| .pdf | 16 |
| .docx | 2 |
| .pptx | 2 |

## Pacotes mais relevantes

| Pacote | Itens |
| --- | --- |
| OneDrive_2026-08-19 (3).zip | 692 |
| OneDrive_2026-08-19 (4).zip | 354 |
| OneDrive_2026-08-19 (2).zip | 123 |
| OneDrive_2026-08-19 (7).zip | 110 |
| OneDrive_2026-08-19.zip | 31 |
| OneDrive_2026-08-19 (1).zip | 13 |
| OneDrive_1_18-08-2026.zip | 7 |
| OneDrive_2026-08-19 (6).zip | 6 |
| OneDrive_2026-08-19 (5).zip | 5 |
| OneDrive_2026-08-19/Casa civil/2023-05-Maio/2023_05_mcmv_ogu_dados.zip | 1 |
| OneDrive_2026-08-19/Casa civil/2023-06-Junho/2023_06_mcmv_ogu_dados.zip | 1 |
| OneDrive_2026-08-19/Casa civil/2023-07-Julho/2023_07_mcmv_ogu_dados.zip | 1 |
| OneDrive_2026-08-19/Casa civil/2023-08-Agosto/2023_08_mcmv_fgts_dados.zip | 1 |
| OneDrive_2026-08-19/Casa civil/2023-08-Agosto/2023_08_mcmv_fgts_entregas_dados.zip | 1 |
| OneDrive_2026-08-19/Casa civil/2023-08-Agosto/2023_08_mcmv_ogu_dados.zip | 1 |

## Conclusao

- O SharePoint local contem massa forte para FAR, Entidades/FDS, Rural, FGTS financiado, OGU subsidiado e entregas FGTS.
- Nao apareceram pacotes explicitos e volumosos de Pro-Moradia ou Reforma Casa Brasil no acervo local inventariado.
- O acervo local e util como trilha de auditoria e fonte complementar, mas a producao deve consumir o que foi publicado no MinIO em `raw/sharepoint/`.
