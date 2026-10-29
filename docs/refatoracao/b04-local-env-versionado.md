# B4 · `local.env` versionado

> Parte do [guia das refatorações](README.md) · Prioridade P0 · Status: a abrir · Issue: — · PR: —

## Problema (✔ conferido no código)

- **Versionado, apesar do `.gitignore`.** `local.env` está no `.gitignore:36`, mas foi removido em `b9c14d0` e re-adicionado em `e1c1ed3`.
- **Conteúdo:** uma Fernet key e credenciais de dev do Postgres e do MinIO. Os campos do DW (`DB_DW_*_MCID`) e do SFTP estão vazios.
- **Três scripts carregam o arquivo:**
  - `scripts/sftp_minio_diff.py:38`;
  - `scripts/superset/linha_financiada.py:724`;
  - `dbt/mcid/_run-common.sh:52`.

  Quem preencher o arquivo com as credenciais reais do DW ou do SFTP vai commitá-las.

## Correção proposta

- `git rm --cached local.env`.
- Criar `local.env.example` só com as chaves.
- Ajustar os três scripts para orientar a cópia do exemplo.
- Trocar a Fernet key de dev, porque a atual fica no histórico.

## Critério de aceite

- `git ls-files local.env` vazio.
- Os três scripts funcionam com um `local.env` local criado a partir do exemplo.
