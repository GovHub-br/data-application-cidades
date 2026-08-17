# Issue #117 — Status: ADR de Arquitetura de Producao Pendente

## Resumo para colar na issue

A issue #117 ainda nao foi concluida. O trabalho feito ate agora ajuda a embasar a decisao arquitetural, mas o ADR formal ainda precisa ser redigido, revisado e aprovado pelo time.

## O que ja existe como insumo

- POC operacional com MinIO/SFTP como origem de staging/raw.
- Diretriz corrigida: modelos dbt da silver MCMV devem ler MinIO `staging/` via DuckDB; Postgres pode ser destino analitico, nao fonte da silver.
- Separacao conceitual validada:
  - MinIO: raw/staging e preservacao de origem.
  - Bronze: copia fiel/projecao minima da staging.
  - Silver: dados tratados, padronizados e governados.
  - Gold/marts: consumo de dashboard, Superset e planilhas.
- Evidencias da issue #119 para padrao silver.
- Evidencias historicas da issue #118/#66 para necessidade de snapshot e consulta temporal.

## Arquitetura alvo sugerida para o ADR

```text
Fonte
  -> MinIO raw/staging
  -> Airflow
  -> DuckDB lendo staging/
  -> dbt
  -> silver tratada
  -> Postgres gold/marts
  -> Superset / planilhas / relatorios
```

## Decisao a registrar

Recomendacao inicial:

- Preservar a origem no MinIO em `raw/` e `staging/`.
- Usar bronze como copia fiel/projecao minima da staging para rastreabilidade.
- Gerar silver por DuckDB a partir de `staging/`, com tratamento, padronizacao semantica e contrato comum por frente.
- Usar gold/marts para dashboards, evitando que Superset dependa diretamente de tabelas silver instaveis.
- Adotar campos tecnicos de auditoria e historico para reprocessamento e comparacao temporal.

## Alternativas que o ADR deve comparar

- Bronze apenas como copia/projecao da staging no MinIO versus bronze materializada no Postgres.
- Consumo direto da staging versus copia bronze rastreavel.
- Historico por snapshot completo versus incremental/hash de linha.
- Gold/marts como tabelas materializadas versus views.

## Consequencias esperadas

- Mais rastreabilidade para auditoria.
- Reprocessamento mais simples quando a origem muda.
- Menos acoplamento entre dashboard e staging.
- Custo adicional de armazenamento/processamento para manter bronze e historico.
- Necessidade de observabilidade para cargas, datas maximas, contagem de linhas e falhas.

## Proximo passo objetivo

Criar o ADR em `docs/adr/` ou pasta equivalente do projeto, usando a estrutura:

- Contexto
- Decisao
- Alternativas consideradas
- Trade-offs
- Consequencias
- Plano de migracao
- Validacao operacional

Esta issue deve ficar aberta ate o ADR existir no repositorio e ser revisado pelo time.
