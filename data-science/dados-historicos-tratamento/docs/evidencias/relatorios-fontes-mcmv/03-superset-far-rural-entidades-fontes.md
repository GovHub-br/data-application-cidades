# Relatorio 03 - Superset FAR/RURAL/Entidades x MinIO/SharePoint

- Gerado em: `2026-08-19 03:25:03 UTC`
- Dashboards: `3`
- Charts: `85`
- Datasets distintos: `12`
- Datasets com fonte candidata no SharePoint local: `12/12`
- Datasets com fonte candidata no MinIO raw: `12/12`
- Datasets com fonte candidata no MinIO `raw/sharepoint/`: `12/12`
- Datasets com valor real amostrado encontrado no SharePoint: `9/12`

## Charts por dashboard

| Dashboard | Frente | Charts |
| --- | --- | --- |
| Entidades | entidades | 34 |
| FAR | far | 24 |
| RURAL | rural | 27 |

## Datasets que alimentam os dashboards

| Dashboard | Schema | Tabela | Frente dataset | Charts | Linhas |
| --- | --- | --- | --- | --- | --- |
| Entidades | empreendimento_far | panorama_estadual | far | 1 | 1366 |
| Entidades | entidades_fds | fds_ficha_empreendimento | entidades | 31 | 343 |
| Entidades | entidades_fds | fds_evolucao_financeira_chart | entidades | 2 | 306 |
| FAR | empreendimento_far | ficha_empreendimento | far | 10 | 1646 |
| FAR | empreendimento_far | panorama_estadual | far | 11 | 1366 |
| FAR | empreendimento_far | mapa_nacional | far | 2 | 32 |
| FAR | empreendimento_far | execucao_fisica_financeira_chart | far | 1 | 17366 |
| RURAL | empreendimento_rural | ficha_empreendimento_rural | rural | 14 | 10402 |
| RURAL | empreendimento_rural | panorama_estadual_rural | rural | 8 | 326 |
| RURAL | empreendimento_rural | execucao_fisica_financeira_chart_rural | rural | 1 | 59004 |
| RURAL | empreendimento_rural | mapa_nacional_rural | rural | 2 | 32 |
| RURAL | empreendimento_rural | perfil_beneficiarios | rural | 1 | 103 |
| RURAL | empreendimento_rural | infraestrutura_agua_saneamento | rural | 1 | 10402 |

## Fonte por dataset

| Schema | Tabela | SharePoint | MinIO raw | MinIO raw/sharepoint |
| --- | --- | --- | --- | --- |
| empreendimento_far | ficha_empreendimento | True | True | True |
| empreendimento_far | panorama_estadual | True | True | True |
| empreendimento_far | mapa_nacional | True | True | True |
| empreendimento_far | execucao_fisica_financeira_chart | True | True | True |
| entidades_fds | fds_ficha_empreendimento | True | True | True |
| entidades_fds | fds_evolucao_financeira_chart | True | True | True |
| empreendimento_rural | ficha_empreendimento_rural | True | True | True |
| empreendimento_rural | panorama_estadual_rural | True | True | True |
| empreendimento_rural | execucao_fisica_financeira_chart_rural | True | True | True |
| empreendimento_rural | mapa_nacional_rural | True | True | True |
| empreendimento_rural | perfil_beneficiarios | True | True | True |
| empreendimento_rural | infraestrutura_agua_saneamento | True | True | True |

## Alerta encontrado

| Dashboard | Schema | Tabela | Frente dashboard | Frente dataset | Charts |
| --- | --- | --- | --- | --- | --- |
| Entidades | empreendimento_far | panorama_estadual | entidades | far | 1 |

## Conclusao

- Os dados que alimentam os dashboards FAR, RURAL e Entidades existem no PostgreSQL e possuem fonte candidata no SharePoint e no MinIO.
- O match direto por nome nao deve ser usado como unico criterio, porque o Superset consome tabelas analiticas/agregadas e as fontes tem nomes operacionais.
- A correcao imediata e revisar o chart `Estado` de Entidades, que hoje aponta para `empreendimento_far.panorama_estadual`.
