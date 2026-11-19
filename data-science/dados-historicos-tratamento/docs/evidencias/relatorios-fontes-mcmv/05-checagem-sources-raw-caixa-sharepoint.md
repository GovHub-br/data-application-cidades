# Relatorio 05 - Checagem sources raw CAIXA/FAR/IBGE no SharePoint e MinIO

- Escopo: fontes declaradas no `sources.yml` para FAR, dados prioritarios da CAIXA e referencias IBGE.
- SharePoint local: `/home/juan-pablo/CIDADES/sharepoint`, incluindo conteudo de ZIPs inventariado.
- MinIO oficial do SharePoint: `data-lake-mcid/raw/sharepoint/`.
- Validacao de linhas/colunas feita com pandas lendo os CSVs exatos do MinIO.

## Resultado resumido

| Source | Local exato | Local equivalente | MinIO raw/sharepoint exato | Linhas | Colunas |
|---|---:|---:|---:|---:|---:|
| `novo_mcmv_far_consolidado` | 0 | 146 | 1 | 15091 | 97 |
| `novo_mcmv_far_cad_pj_mensal` | 0 | 72 | 1 | 822 | 82 |
| `novo_mcmv_far_obra_mensal` | 0 | 73 | 1 | 822 | 40 |
| `novo_mcmv_far_financeiro_mensal` | 0 | 73 | 1 | 6069 | 23 |
| `dados_prioritarios_recebidos_caixa_empreendimentos` | 0 | 0 | 1 | 14923 | 42 |
| `dados_prioritarios_recebidos_caixa_entregas` | 0 | 0 | 1 | 11540 | 7 |
| `api_ibge_uf` | 0 | 0 | 1 | 27 | 8 |
| `api_ibge_regioes` | 0 | 0 | 1 | 5 | 5 |

## Evidencias por source

### `novo_mcmv_far_consolidado`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/novo_mcmv_far_consolidado.csv`
- Equivalentes locais: 146. OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/HIS_MCIDADES_CONSOLIDADO_20260209.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250312.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Consolidado/HIS_MCIDADES_CONSOLIDADO_20250312.txt
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/novo_mcmv_far_consolidado.csv
- Perfil pandas: 15091 linhas, 97 colunas, separador `,`.
- Colunas iniciais: `co_tipo_registro; dt_recebimento_gfar; dt_protocolo; hr_protocolo; no_agente_financeiro; no_identificacao_proposta; co_fase; co_etapa; co_status; no_titularidade_imovel; no_nome_empreendimento; no_logradouro_empreendimento; no_bairro; co_cep; no_municipio; no_uf; no_regiao; co_municipio_ibge; co_tipo_edificacao; ic_mapeamento_imovel`

### `novo_mcmv_far_cad_pj_mensal`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv`
- Equivalentes locais: 72. OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FAR_20250505_040201.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FAR_20250507_040108.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Cad PJ/MONIT_CAD_PJ_FAR_20250508_040104.csv
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/novo_mcmv_far_cad_pj_mensal.csv
- Perfil pandas: 822 linhas, 82 colunas, separador `,`.
- Colunas iniciais: `co_tipo_registro; dt_movimento; nu_apf; no_identificacao_proposta; no_agente_financeiro; no_empreendimento; dt_apresentacao_orcamento; ic_terreno_doado; no_logradouro_emprrendimento; no_bairro; co_cep; no_municipio; sg_uf; co_municipio_ibge; no_construtora; nu_cnpj_construtora; co_ente_publico_proponente; dt_contratacao; dt_inicio_obra; dt_previsao_conclusao_obra`

### `novo_mcmv_far_obra_mensal`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/novo_mcmv_far_obra_mensal.csv`
- Equivalentes locais: 73. OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/Arquivados/Arquivo 2025 - Obra/MONIT_MOV_OBRA_FAR_20250424_131326.csv_Error.txt | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/Arquivados/Arquivo 2025 - Obra/MONIT_MOV_OBRA_FAR_20250505_040205.csv_Error.txt | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/Arquivados/Arquivo 2025 - Obra/MONIT_MOV_OBRA_FAR_20250507_040111.csv_Error.txt
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/novo_mcmv_far_obra_mensal.csv
- Perfil pandas: 822 linhas, 40 colunas, separador `,`.
- Colunas iniciais: `co_tipo_registro; dt_movimento; nu_apf; co_situacao_obra; dt_alteracao_situacao; ic_invadido; dt_invasao; pc_obra_prevista; pc_obra_realizada; qt_uh_concluidas; qt_uh_concluidas_adaptadas; dt_previsao_conclusao_obra_retomada; dt_conclusao_obra_retomada; dt_primeira_assinatura_pf; dt_ultima_assinatura_pf; qt_uh_alienada; vr_total_uh_alienadas; nu_qt_uh_a_alienar; vr_total_uh_a_alienar; qt_uh_sem_habitese`

### `novo_mcmv_far_financeiro_mensal`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/novo_mcmv_far_financeiro_mensal.csv`
- Equivalentes locais: 73. OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FAR_20250505.csv | OneDrive_2026-08-19 (3).zip::Novo MCMV - FAR/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FAR_20250520.csv | OneDrive_2026-08-19 (3).zip::__Novo MCMV - FAR/Arquivados/Arquivo 2025 - Financeiro/MONIT_MOV_FINANC_FAR_DIARIO_20251230_103003.CSV_Error.txt
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/novo_mcmv_far_financeiro_mensal.csv
- Perfil pandas: 6069 linhas, 23 colunas, separador `,`.
- Colunas iniciais: `co_tipo_registro; dt_movimento; nu_apf; dt_remessa; dt_liberacao_recurso; co_tipo_movimento; co_tipo_lib_recurso; vr_movimento; ic_credito; vr_pago_obra_empreendimento; vr_pago_terreno; vr_pago_pts; vr_pago_equipamentos_publicos; vr_pago_aporte_suplementacao; vr_pago_despesas_manutencao; vr_pago_despesas_incc; vr_pago_cartorios_legalizacao; vr_liberado; no_identificador; dt_evento`

### `dados_prioritarios_recebidos_caixa_empreendimentos`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/dados_prioritarios_recebidos_caixa_empreendimentos.csv`
- Equivalentes locais: 0. Sem equivalente local pelo padrao pesquisado.
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/dados_prioritarios_recebidos_caixa_empreendimentos.csv
- Perfil pandas: 14923 linhas, 42 colunas, separador `,`.
- Colunas iniciais: `data_de_movimento; agente_financeiro; apf; uf; municipio; codigo_ibge_do_municipio; nome_empreendimento; modalidade; situacao_do_empreendimento; detalhamento_da_situacao_do_empreendimento; data_de_contratacao; percentual_exec; valor_contratado; valor_aporte_adicional; valor_desembolsado; uh_contratadas; uh_entregues; uh_vigentes; observacoes; quantidade_de_uhs_vigentes_em_janeiro_do_ano_de_referencia`

### `dados_prioritarios_recebidos_caixa_entregas`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/dados_prioritarios_recebidos_caixa_entregas.csv`
- Equivalentes locais: 0. Sem equivalente local pelo padrao pesquisado.
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/dados_prioritarios_recebidos_caixa_entregas.csv
- Perfil pandas: 11540 linhas, 7 colunas, separador `,`.
- Colunas iniciais: `data_de_movimento; agente_financeiro; apf; dt_entrega; qt_uh_entregues; arquivo_de_origem; criado_em`

### `api_ibge_uf`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/api_ibge_uf.csv`
- Equivalentes locais: 0. Sem equivalente local pelo padrao pesquisado.
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/api_ibge_uf.csv
- Perfil pandas: 27 linhas, 8 colunas, separador `,`.
- Colunas iniciais: `id; sigla; nome; regiao_id; regiao_sigla; regiao_nome; arquivo_de_origem; criado_em`

### `api_ibge_regioes`
- Status: existe em `raw/sharepoint/`; nao existe com nome canonico no SharePoint local baixado.
- Objeto MinIO: `raw/sharepoint/api_ibge_regioes.csv`
- Equivalentes locais: 0. Sem equivalente local pelo padrao pesquisado.
- Historico/equivalente no MinIO raw: 1. raw/sharepoint/api_ibge_regioes.csv
- Perfil pandas: 5 linhas, 5 colunas, separador `,`.
- Colunas iniciais: `id; sigla; nome; arquivo_de_origem; criado_em`

## Conclusao

- Os 8 sources existem com nome exato no MinIO `raw/sharepoint/`.
- No SharePoint local baixado, os nomes canonicos nao aparecem; para FAR aparecem equivalentes operacionais (`HIS_MCIDADES_CONSOLIDADO`, `MONIT_CAD_PJ_FAR`, `MONIT_MOV_OBRA_FAR`, `MONIT_MOV_FINANC_FAR`).
- Os dados prioritarios da CAIXA nao apareceram com nome canonico nem com o nome de origem dentro dos ZIPs locais baixados, mas estao publicados no MinIO `raw/sharepoint/` e tambem ha serie historica/arquivos equivalentes em `raw/sftp/` e `raw/dados_historicos/`.
- Para dbt, esses sources devem apontar para os CSVs canonicos em `raw/sharepoint/`, mantendo os objetos SFTP/historicos como trilha de auditoria e reprocessamento.
