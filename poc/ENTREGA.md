### Resumo da Entrega: POC — Bronze em Parquet Tipado no MinIO

Finalizada a prova de conceito que avalia se o MinIO deve ser adotado como armazenamento da
camada bronze em produção. O ponto de partida foi um problema concreto identificado na
arquitetura atual: existem hoje "duas bronzes" — o parquet all-text em `staging/` no MinIO e
a bronze tipada em tabelas Postgres (`sftp.*`) — diferindo só pela tipagem, o que duplica
armazenamento sem necessidade. A POC testou uma arquitetura alternativa: a bronze passa a
ser **parquet já tipado no MinIO**, exposto ao Postgres como view (via `pg_duckdb`), sem
staging intermediário e sem uma tabela por arquivo.

Toda a POC roda isolada em `poc/` (infraestrutura própria via `docker-compose.yml` — MinIO +
Postgres com `pg_duckdb` — sem tocar em nenhum componente do pipeline atual).

## O que foi construído e testado

**Pipeline completo, ponta a ponta, com dados reais do lake do MCid:**

```
raw/ (SFTP)  →  manifesto (encoding/delimitador/família)  →  bronze (parquet tipado, dbt-duckdb)
             →  view (pg_duckdb, 0 bytes)  →  silver (dbt-postgres, dialeto inalterado)
```

- **Manifesto** (`scripts/gerar_manifesto.py`) — reusa `lake_utils.py` do projeto para
  detectar encoding/delimitador/header, agrupa 2.703 arquivos reais em **641 famílias**
  (mesmo layout, datas diferentes) e grava tudo com a mesma chave de idempotência do
  `_staging_log` de hoje: `(minio_key, source_hash)`.
- **Bronze** (`dbt_poc/`, dbt-duckdb) — codegen (`gerar_models_bronze.py`) que transforma o
  manifesto + um YAML curto por família (`dbt_poc/tipos/*.yml`, o único artefato humano) em
  um model `external` particionado. Testado com a família real
  `CAIXA_AF_GEHIS_ANDAMENTO_OBRA` (16 arquivos, 2 encodings, 3 delimitadores, 1 header
  corrompido) → **4.241 linhas, contagem idêntica ao pipeline atual**.
- **View** (`scripts/gerar_views_bronze.py`) — gera o `CREATE VIEW` a partir do footer do
  parquet (mesmo padrão de `staging_para_db.py`, trocando `CAST(...AS VARCHAR)` fixo pelo
  tipo real). **0 bytes** — não materializa nada.
- **Silver** (`dbt_silver/`, dbt-postgres) — model comum, dialeto Postgres, sem nenhuma
  referência a DuckDB/S3/encoding → **3.738 linhas**. Prova que a silver não muda de forma.
- **Incremental** — `partition_by` + `overwrite_or_ignore` com o model gerado só sobre o
  lote novo: 2 rodadas, **0 partições perdidas, 0 reescritas à toa**.

## Achados de peso (documentados em `poc/README.md` e `poc/resultados/`)

- **Redução de armazenamento medida na família real: 96,4%** (`resultados/medicoes_familia.md`),
  a maior parte por eliminar uma tabela Postgres por arquivo (2.011 tabelas em produção
  hoje); o efeito isolado da tipagem é 14,3% (`resultados/medicoes.md`) — número honesto,
  sem inflar o resultado.
- **Bloqueador de encoding (bytes C1) testado e reduzido**: era tratado como bloqueio duro;
  ao investigar, apareceram 2 problemas empilhados — 4 de 5 arquivos eram falso alarme (bug
  de ordem em `lake_utils.detectar_encoding`, raiz do mojibake `ca3digo_cliente` já visto
  antes) e só 1 exige reescrita real, com fidelidade célula a célula comprovada
  (`resultados/bloqueador_c1.md`).
- **Dois defeitos reais de dado nos arquivos do lake, tratados na bronze e não no pipeline
  atual**: header com menos campos que os dados, e header/dados com delimitadores
  diferentes — ambos hoje produzem perda silenciosa em produção.
- **Restrições medidas do `pg_duckdb`**: UDFs Postgres não funcionam sobre a view (a query
  roda inteira no DuckDB); `hive_types` e `=` em argumento nomeado quebram no parser do
  Postgres. Tudo documentado com solução em `poc/README.md`.
- **Orquestração**: as 3 DAGs `astronomer-cosmos` de produção assumem 1 projeto dbt = 1
  adapter por DAG (`type: postgres`); esta arquitetura precisa de 2 adapters (duckdb +
  postgres) na mesma esteira, o que exige recompor as DAGs com `DbtTaskGroup` — mudança
  estrutural identificada, não testada nesta POC (seção dedicada em `poc/README.md`).

## Documentação entregue

- **`poc/README.md`** — veredito completo: medições, achados, bloqueadores, armadilhas do
  `pg_duckdb`, passo a passo de reprodução e o que falta decidir antes de produção.
- **`poc/TUTORIAL.md`** — como fica o dia a dia do desenvolvedor: cenário concreto de 3
  arquivos novos (`.csv`, `.xlsx`, `.txt`) que se unem na silver, do manifesto até o `dbt
  run` da silver.
- **`poc/resultados/`** — todas as medições em markdown (tamanho, incremental, matriz de
  encoding, normalização de nomes, formatos negativos, bloqueador C1, amostra real).

## Checklist da issue

**Tarefas:**
- [x] Selecionar base e volume representativos — família real `CAIXA_AF_GEHIS_ANDAMENTO_OBRA`
      (16 arquivos reais do lake) + dataset sintético de 50.000 linhas cobrindo os formatos
      de data/valor/encoding do MCid.
- [x] Definir estrutura provisória no MinIO — `bronze/<tabela>/<partição>/*.parquet`,
      particionado por Hive; `raw_utf8/` como camada derivada para o resíduo de encoding.
- [x] Implementar carga da fonte para a bronze — `dbt_poc/` (dbt-duckdb), models gerados por
      codegen a partir do manifesto + YAML de tipos.
- [x] Testar leitura e reprocessamento — leitura via view `pg_duckdb`; reprocessamento via
      teste incremental (2 lotes, partições intactas).
- [x] Validar integração com as camadas seguintes — `dbt_silver/` (dbt-postgres) lendo a
      view como source, sem alteração de dialeto.
- [x] Comparar a solução com o fluxo atual — `resultados/medicoes.md` e
      `resultados/medicoes_familia.md`.
- [x] Documentar resultados e recomendação — `poc/README.md`.

**Critérios de aceite:**
- [x] A POC foi executada com uma base representativa.
- [x] Escrita, leitura e reprocessamento foram validados.
- [x] Estrutura de armazenamento e rastreabilidade foi documentada — chave de idempotência
      `(minio_key, source_hash)` no manifesto, mesma lógica do `_staging_log` atual.
- [x] Impactos sobre staging e silver foram identificados — `staging/*.parquet` e as
      tabelas `sftp.*` deixam de existir; a silver não muda de dialeto nem de forma.
- [x] Existe recomendação objetiva para adoção ou não adoção em produção — ver abaixo.

## Recomendação

**Adotar**, com um conjunto pequeno e explícito de pendências antes de produção (todas
detalhadas em `poc/README.md`, seção "O que falta decidir antes de produção"):

1. Corrigir a ordem de checagem em `lake_utils.detectar_encoding` (resolve a maior parte do
   bloqueador de encoding e um bug de mojibake já presente em produção).
2. Upgrade de `dbt-core`/`astronomer-cosmos` e recomposição das DAGs cosmos para dois
   adapters (duckdb + postgres) na mesma esteira.
3. Revisão manual da heurística de agrupamento em famílias (641 famílias, regex sobre nome
   de arquivo) antes de virar contrato.
4. Definir onde o manifesto e o codegen rodam no Airflow (tasks Python antes do `dbt run`).
5. Explicitar o gate de mascaramento de PII na DAG (o dbt não sabe checar a tag
   `masked=true`).

Nenhuma dessas pendências invalida a arquitetura — são itens de implementação, não achados
que reabram a decisão de viabilidade.
