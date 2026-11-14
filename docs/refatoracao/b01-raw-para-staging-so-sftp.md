# B1 · O `raw_para_staging` sobrescreve a staging das APIs

> Parte do [guia das refatorações](README.md) · Prioridade P0 · Status: issue aberta · Issue: [#248](https://github.com/GovHub-br/data-application-cidades/issues/248) · PR: —

## Problema (✔ conferido no código)

`scripts/raw_para_staging.py`, rodado todo dia pelo `minio_transform_dag`:
- varre todo o `raw/` e só ignora `dados_historicos` (`PASTAS_IGNORADAS`, `:99`; varredura em `:1085`);
- grava `staging/<mesmo caminho>.parquet` só com texto, acrescentando `_source_file`, `_ingested_at` e `_source_hash` (`:101`).

Por isso `raw/fgv/icst.csv` vira `staging/fgv/icst.parquet`, e `raw/ibge/<tabela>.json` vira `staging/ibge/<tabela>.parquet`. São os mesmos caminhos que as DAGs de API escrevem tipados.

Em agosto/2026, os parquets do FGV e do infomoney apareceram quebrados exatamente com essas três colunas. A suspeita, na época, era um "job do outro time". **A hipótese forte é que a colisão nasce do nosso próprio script.**

## Correção proposta

- Restringir a varredura a `raw/sftp/`.
- Não mudar o caminho de nenhuma DAG. Criar um namespace próprio na staging já foi rejeitado, porque quebra o data mesh.

## Antes de implementar

- Listar no `lake._staging_log` as chaves de origem fora de `sftp/`, para confirmar a hipótese.
- Conferir se alguma conversão de fora de `sftp/` é consumida, como o `.mdb` ou o `dados_historicos`.

## Critério de aceite

- Um run do `minio_transform_dag` não reescreve `staging/fgv`, `staging/ibge`, `staging/bacen` nem `staging/infomoney`.
- Um teste em `tests/` cobre o filtro de prefixo.

## Relação

Faz par com [B2](b02-mascara-como-trava.md), porque é o mesmo script e os dois podem ir no mesmo PR. Alimenta o ADR do contrato da staging ([F1](f1-adrs.md)) e o [B12](b12-escritor-unico-staging.md).
