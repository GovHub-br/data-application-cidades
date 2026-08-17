# Glossário MCID para Silver e Marts

Este glossário é a referência semântica inicial para padronizar modelos dbt da
frente Cidades. Ele deve ser usado nos `schema.yml`, nomes físicos e marts de
dashboard.

## Camadas

- `raw`: arquivo bruto no MinIO/Object Storage, preservado como recebido da fonte.
- `staging`: arquivo estruturado no MinIO, geralmente Parquet, pronto para leitura por DuckDB/dbt.
- `bronze`: cópia fiel ou projeção mínima da staging para o ambiente analítico. Não deve aplicar regra de negócio.
- `silver`: primeira camada tratada. Deve tipar, normalizar nomes, chaves, datas, valores e campos técnicos. Para o MCMV, deve ser gerada somente a partir do MinIO `staging/` via DuckDB.
- `mart_dashboard`: modelo final exclusivamente consumido por dashboard. Deve ter grão e métricas documentados.
- `gold`: camada analítica final. Pode conter marts de dashboard, indicadores oficiais e agregações publicáveis.

## Termos Canônicos

| Termo | Campo preferencial | Tipo sugerido | Definição |
| --- | --- | --- | --- |
| APF | `apf` | `text` | Identificador da operação/empreendimento. Deve ser normalizado, preservando zeros quando existirem. |
| Contrato | `contrato` ou `nu_contrato` | `text` | Identificador contratual quando a fonte não usa APF. |
| Empreendimento | `nome_empreendimento` | `text` | Nome do empreendimento habitacional. |
| Frente MCMV | `frente_mcmv` | `text` | Frente ou linha estratégica: FAR, Entidades, Rural, Reforma, Classe Média, Cidades, FNHIS, SUB50, Pró-Moradia. |
| Modalidade | `modalidade` | `text` | Modalidade operacional informada pela fonte. |
| Município | `municipio` | `text` | Nome do município padronizado. |
| UF | `uf` | `text` | Sigla da unidade federativa, sempre com 2 caracteres. |
| Código IBGE Município | `codigo_ibge_municipio` | `text` | Código IBGE do município, preservado como texto quando puder conter zeros à esquerda. |
| Unidade Habitacional | `quantidade_uh` | `integer` | Quantidade de UHs contratadas, produzidas ou previstas, conforme grão da fonte. |
| UHs Entregues | `quantidade_uh_entregues` | `integer` | Quantidade de UHs com entrega efetiva. |
| Valor Contratado | `valor_contratado` | `numeric(15,2)` | Valor total contratado, investimento ou operação, conforme documentação do modelo. |
| Valor Desembolsado | `valor_desembolsado` | `numeric(15,2)` | Valor financeiro liberado/desembolsado/evento acumulado quando disponível. |
| Execução Física | `percentual_execucao_fisica` | `numeric(10,2)` | Percentual de avanço físico da obra, de 0 a 100. |
| Execução Financeira | `percentual_execucao_financeira` | `numeric(10,2)` | Percentual desembolsado sobre o valor contratado, de 0 a 100. |
| Status Operacional | `status_operacional` | `text` | Situação operacional consolidada para leitura de dashboard. |
| Data de Referência | `dt_referencia` | `date` | Data de posição da informação. |
| Data de Ingestão | `dt_ingest` | `timestamp` | Data/hora de ingestão no pipeline. |
| Data Silver | `dt_silver` | `timestamp` | Data/hora de materialização na camada silver. |
| Data Gold | `dt_gold` | `timestamp` | Data/hora de materialização na camada gold/mart. |

## Regras Gerais

- Chaves e códigos devem ficar como `text` quando zeros à esquerda forem possíveis.
- Valores monetários devem usar `numeric(15,2)` ou precisão superior documentada.
- Percentuais devem ser documentados como escala `0-100` ou decimal; o padrão MCID é `0-100`.
- Marts de dashboard devem informar consumidor, grão, filtros esperados e campos usados por cards/mapas/tabelas.
- Mudanças de nome em modelos consumidos por dashboard devem preferir alias/view de compatibilidade antes de renome físico.
