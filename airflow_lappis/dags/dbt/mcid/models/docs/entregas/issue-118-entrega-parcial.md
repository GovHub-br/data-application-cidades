# Issue #118 — Entrega Parcial: Estrategia para Dados Historicos

## Resumo para colar na issue

Foi entregue uma base piloto para uso historico e analise temporal do MCMV, conectada ao trabalho da silver padronizada e aos dados historicos usados nos indicadores. A issue ainda nao deve ser considerada totalmente fechada, porque falta formalizar a estrategia unica de retencao, versionamento, snapshots e reprocessamento.

## O que foi entregue

- Serie temporal anual de unidades habitacionais contratadas.
- Separacao entre OGU/subsidiado e FGTS/financiado para evitar conclusao incorreta sobre ausencia total de contratacao.
- Evidencia visual do hiato/reducao em OGU/subsidiado entre 2019 e 2023, enquanto FGTS financiado continua existindo.
- CSV de apoio para auditoria da serie historica.
- Modelos silver com campos de data e campos tecnicos para consultas temporais futuras.
- Base consolidada `silver_mcmv_frentes_base` para cruzamentos por frente.

## Evidencias no repositorio

- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-uh-contratadas.csv`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-ogu-barras-fgts-linha.png`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-ogu-subsidiado-area.png`
- `airflow_lappis/dags/dbt/mcid/models/indicadores_mcmv_dbt/docs/evidencias/issue-66-serie-temporal-mcmv-ogu-subsidiado-barras-com-eixo-valores.png`
- `airflow_lappis/dags/dbt/mcid/models/docs/issue-119-padrao-silver-marts-dashboard.md`
- `airflow_lappis/dags/dbt/mcid/models/mcmv_silver_dbt/silver/`

## Fontes usadas no piloto

- `__dados_brutos.dados_abertos_mcmv_ogu_empreendimentos`
- `__dados_brutos.dados_abertos_mcmv_fgts_sintetico`

## Leitura dos dados

A evidencia mostra que a linha OGU/subsidiado cai para zero ou quase zero em parte da janela historica recente. Isso nao significa que todo financiamento habitacional deixou de existir, porque a serie FGTS/financiado continua com volume relevante no mesmo periodo. Para dashboard e alertas, as duas linhas precisam ser tratadas separadamente:

- OGU/subsidiado: importante para metas subsidiadas, FAR, Rural, Entidades, FNHIS/SUB50 e entregas com recurso publico.
- FGTS/financiado: importante para Classe Media/Faixa 3, conjuntura habitacional e calibragem de demanda.

## O que ainda falta para fechar a issue

- Definir formalmente quais bases exigem historico.
- Registrar granularidade temporal por base: diaria, semanal, mensal, anual ou por evento.
- Definir retencao, particionamento e versionamento.
- Criar padrao unico de campos tecnicos, por exemplo:
  - `dt_ingest`
  - `dt_referencia`
  - `source_file`
  - `source_path`
  - `snapshot_date`
  - `hash_linha`
  - `dt_valid_from`
  - `dt_valid_to`
  - `is_current`
- Implementar uma base piloto versionada com teste de reprocessamento.
- Documentar operacao de correcao retroativa e exclusao na origem.

## Recomendacao de encaminhamento

Usar esta entrega como evidencia inicial da necessidade de historico e abrir a etapa seguinte para formalizar o padrao de snapshot/versionamento, alinhado ao ADR da issue #117.
