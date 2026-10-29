# F1 · Organização do desenvolvimento via ADRs

> Parte do [guia das refatorações](README.md) · Frente prevista · Status: issue aberta · Issue: — · PR: —

## Objetivo

Registrar em ADR as decisões de arquitetura antes das refatorações que mudam contrato, para que a ingestão (F2) e o dbt (F3) partam de regras escritas, não de memória.

## Onde

Os ADRs são escritos em outra branch. Esta frente **não** cria nem edita `docs/adr` aqui: este arquivo só acompanha quais decisões precisam de ADR.

## Decisões que precisam de ADR

| Decisão | Por quê | Bloqueia |
|---|---|---|
| **Contrato da `staging/`**: quem escreve, tipado ou só texto, um escritor só | Hoje há três escritores com regras diferentes, e o `raw_para_staging` sobrescreve a staging das APIs | [F2](f2-ingestao.md), [B1](b01-raw-para-staging-so-sftp.md), [B12](b12-escritor-unico-staging.md) |
| **Destino das tabelas Postgres "raw" das APIs** (`abecip.*`, `bacen.*`, `fgv.*`…) | Nenhum modelo dbt lê essas tabelas: as sources de `dbt/mcid/models/sources.yml:4-121` têm 0 usos. Só o infomoney lê o próprio histórico | [F2](f2-ingestao.md) (histórico truncado na staging) |
| **Nomenclatura** `prata/ouro` × `silver/gold` | `dbt/mcid/models/docs/padrao-nomenclatura-tabelas-dbt.md` manda `silver/gold`, mas o código usa `prata/ouro` | [F3](f3-dbt.md) |

## Antes de decidir o Postgres raw

Conferir no Superset quais datasets ainda apontam para os schemas `abecip`, `bacen`, `fgv`, `fipe`, `ibge`, `infomoney`, `mrv`, `novo_caged` e `siafi`.

## Critério de aceite

Os três ADRs estão aprovados e o [registro de decisões do guia](README.md#8-registro-de-decisões) aponta para cada um.
