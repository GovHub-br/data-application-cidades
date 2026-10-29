# F5 · Melhorias no OpenMetadata

> Parte do [guia das refatorações](README.md) · Frente prevista · Status: a abrir · Issue: — · PR: —

## Objetivo

Deixar o catálogo do OpenMetadata completo e a configuração dele sem valores fixos espalhados.

## O que a varredura encontrou

- **O eixo histórico está fora do catálogo.** mcmv_historico, reforma e linha_financiada não têm catálogo nem tipos de coluna, porque o `docs generate` usa `--selector dominios_postgres` (`helpers/openmetadata/dbt_artifacts.py:18`). A correção vem da [F3](f3-dbt.md).
- **A versão 1.13.3 está fixada em quatro lugares:**
  - `requirements.txt:50` (client);
  - `infra/playbooks/openmetadata/group_vars/openmetadata.yaml:9` (VM, com o Elasticsearch 9.3.0 em `:11`);
  - `infra/k8s/openmetadata/values.yaml` (chart);
  - `infra/k8s/openmetadata/elasticsearch.yaml:37`.
- **Valores fixos:** o serviço "Cidades" e o e-mail de admin estão em `infra/playbooks/airflow/templates/airflow.env.j2:39` e em `infra/k8s/openmetadata/bootstrap.yaml`.
- **Leitura no parse:** os toggles `OM_INGEST_*` são lidos no parse do DAG `dags/openmetadata_ingestion_dag.py`.
- **Comentários desatualizados:** dizem que o `openmetadata-ingestion` roda num venv isolado (`pyproject.toml:147-149`, `infra/env/.env.example:58-62`), mas ele está na imagem.
- **Exemplo do IPEA:** `helpers/openmetadata/README.md:168-205` ainda usa `compras_gov_api` e `api_contratos_dag`.

## Pendências de ambiente já conhecidas

- **Homolog:** a DAG do OM ainda falha no homolog, e o Cosmos está pausado lá.
- **Pipeline client e token:** o pipeline client do OM está desligado e o token do bot precisa ser rotacionado.

## Depende de

[F3](f3-dbt.md), para o catálogo completo.

## Critério de aceite

- Todos os domínios do dbt aparecem no OM com linhagem.
- A versão do OM é definida num lugar só por ambiente.
- Nenhum valor de serviço ou de e-mail está fixo nos templates.
