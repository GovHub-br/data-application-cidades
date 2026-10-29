# B2 · A máscara não trava o caminho da PII

> Parte do [guia das refatorações](README.md) · Prioridade P0 · Status: a abrir · Issue: — · PR: —

## Problema (✔ conferido no código)

- **A falha não para a DAG.** `scripts/mascarar_minio.run` registra a falha de cada objeto como `status="error"` e retorna normalmente. O `minio_transform_dag` só loga a contagem.
- **A staging não confere a máscara.** `scripts/raw_para_staging.py` não lê a tag `masked`; só o `.mdb` é remascarado.
- **O erro de leitura vira "não mascarado".** `ClienteMinio.get_tagging` engole o erro (`plugins/cliente_minio.py:371-376`).
- **Os schedules não estão amarrados.** O `sftp_ingest_dag` e o `minio_transform_dag` rodam em horários independentes. Um arquivo que chega ao `raw/` entre a máscara e a conversão vira parquet sem ter sido mascarado.

## Correção proposta

- A conversão para a staging exige a tag `masked=true` ou o registro do hash no `lake._masking_log`.
- A task de máscara falha quando houver erro em qualquer objeto.
- O `get_tagging` propaga o erro em vez de devolver "não mascarado".

## Critério de aceite

Testes em `tests/` mostrando que:
- um objeto sem `masked` não vira parquet;
- um erro de máscara falha a task.

## Relação

Faz par com [B1](b01-raw-para-staging-so-sftp.md).
