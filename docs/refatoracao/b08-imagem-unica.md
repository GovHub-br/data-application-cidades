# B8 · Imagem da CI que ninguém usa

> Parte do [guia das refatorações](README.md) · Prioridade P1 · Status: issue aberta · Issue: [#255](https://github.com/GovHub-br/data-application-cidades/issues/255) · PR: —

## Problema

- **Três nomes de imagem:**
  - a CI publica `ghcr.io/govhub-br/data-application-cidades-airflow:{sha,latest}` (`.github/workflows/main.yaml:25,116-117`);
  - o k8s espera `ghcr.io/govhub-br/cidades-airflow:main` (`infra/k8s/airflow/values.yaml:10-11`), publicada à mão;
  - a VM builda local como `cidades-airflow:latest` (`infra/playbooks/airflow/install-airflow.yaml:10,122-129`).
- **Build em dobro:** os jobs `docker_build` e `docker_push` buildam a imagem duas vezes.
- **Contexto inchado:** não há `.dockerignore`, então o contexto do build leva `data-science/` (32 MB) e o `.git`.
- **Conteúdo indevido na imagem:**
  - `infra/airflow/simple_auth_manager_passwords.json` (airflow/airflow) vai dentro dela (`Dockerfile:44`);
  - libs de sistema sem uso: gdal, odbc, gtk (`Dockerfile:4-18`);
  - o OpenSSL é rebaixado para TLSv1.0/`SECLEVEL=1`, e o bundle da ICP-Brasil é baixado por HTTP (`Dockerfile:19-21`).
- **O pg_duckdb do k8s é buildado à mão** (`infra/k8s/postgres/Dockerfile`) e não tem CI.

## Correção proposta

- Um nome de imagem só, publicado pela CI e consumido pela VM e pelo k8s.
- Build único, com push do mesmo artefato.
- Um `.dockerignore`.
- A senha padrão fora da imagem.
- Libs sem uso removidas.

## Critério de aceite

- A VM e o k8s sobem com a tag publicada pela CI.
- O contexto do build não inclui `data-science/` nem `.git`.
