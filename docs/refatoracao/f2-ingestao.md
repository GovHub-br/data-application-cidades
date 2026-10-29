# F2 · Refatoração das ingestões: providers primeiro, depois clientes próprios

> Parte do [guia das refatorações](README.md) · Frente prevista · Status: a abrir · Issue: — · PR: —

## Objetivo

Remodelar as DAGs de ingestão do zero:
1. primeiro sobre os hooks dos providers do Airflow (HTTP, S3, SFTP, IMAP, Postgres);
2. depois, só onde o provider não basta, com clientes de código próprios, como os atuais.

## Decisão de 29/10/2026

O projeto anterior foi **descartado**. Era o PR #206, na branch `refactor/ingestao-raw-staging-bronze`, com o pacote `plugins/ingestion` de extratores, conversores, estratégias e `DatasetSpec`. Os passos antes de começar:

1. Fechar o PR #206 e apagar a branch. É irreversível, então só com confirmação explícita na hora.
2. Modelar em plan mode, numa sessão própria, e mostrar um exemplo de DAG antes de replicar.

## Regras do #206 a revalidar

Elas não são herdadas. Cada uma será perguntada na modelagem, não assumida:

- o bronze só pelo dbt (`fonte_lake`), sem carga em Python;
- nada carrega no Postgres; quem lê a staging é o pg_duckdb;
- o lake espelha a estrutura da fonte (SFTP com os caminhos do servidor);
- a saída de cada DAG é igual à da `main`: caminhos, formato, colunas e controles;
- só o escopo MCid;
- o código em inglês e as docs e os commits em português;
- hooks dos providers, sem operators de transferência (eles põem o dado em memória ou no XCom);
- cron literal, sem `get_dynamic_schedule`;
- o que fazer com DAGs quase idênticas: as três do CAGED, as duas do MRV, abecip, poupança e fipezap, e icst com incc.

## O que o modelo novo precisa resolver

- **Clientes duplicados:**
  - dois de IBGE (`plugins/cliente_ibge.py` v3 e `plugins/cliente_ibge_sidra.py`) e dois de BCB (`cliente_bacen`, `cliente_bacen_imobiliario`);
  - três pilhas HTTP: httpx, requests e requests com SSL `SECLEVEL=1` (`plugins/cliente_fgv.py:23`);
  - MinIO montado de cinco jeitos;
  - Postgres por dois caminhos: o hook `postgres_default` e um DSN próprio em cada script;
  - SFTP com quatro implementações em paramiko.
- **DAG SIDRA órfão.** `pnad_construcao_sidra_ingest_dag` grava em `staging/ibge/pnad_construcao_*`, que nenhuma source lê. O PNAD já vem pelo `ibge_ingest_dag`.
- **"Sem dado" termina verde.** icst, incc, mrv, caged, bacen, credito_pib, ibge e sidra só registram warning. Só abecip e fipe falham com `AirflowFailException`.
- **Histórico truncado na staging** enquanto o Postgres acumula: bacen `ultimos=13`, sidra 12, ibge `-20`. Depende do [ADR do Postgres raw](f1-adrs.md).
- **Tesouro não é idempotente.**
  - Faz append sem chave.
  - Só procura o e-mail de "hoje".
  - Passa o CSV inteiro pelo XCom.
  - Faz monkeypatch em `cliente_email.format_csv` (`dags/data_ingest/tesouro_gerencial/dotacao_execucao_outras_fontes_mcid_ingest_dag.py:117,137,162`).
- **Código no parse do DAG:**
  - `Variable.get("IBGE_CONFIGURACOES")` no topo do módulo;
  - `get_dynamic_schedule` lendo Variable em 20 DAGs, com chaves que não batem com o `dag_id`;
  - `os.environ['AIRFLOW_REPO_BASE']` e `os.makedirs` no topo dos DAGs de dbt.
- **Imports de Airflow 2:**
  - `dags/data_ingest/minio/minio_transform_dag.py:4-6`;
  - `dags/dbt/mcid_cosmos_dag.py:40-42`, que importa `from airflow import DAG` e depois sobrescreve com o SDK.
- **Segredos em Variables** que deviam ser Connections: `email_credentials`, `dados_fgv_*` e `api_key_alphavantage`.
- **Conexões e logs:**
  - `helpers/postgres_helpers.py:8-18` nunca fecha a conexão;
  - `plugins/cliente_postgres.py:52-55` loga o DSN com a senha;
  - a apikey da Alpha Vantage vai na query string, que o httpx loga.

## Itens que esta frente absorve

[B11](b11-codigo-morto-ingestao.md) (código morto), [B12](b12-escritor-unico-staging.md) (escritor único da staging) e [B13](b13-orquestracao.md) (orquestração).

## Depende de

- [F1](f1-adrs.md): contrato da staging e destino do Postgres raw.
- [F4](f4-uv.md) e [B6](b06-ci-blocante.md), para que a frente já nasça com lint e DagBag na CI.

## Critério de aceite (por DAG migrada)

- A saída é igual à da `main`, comparada em `tests/`, sem tocar `raw/` e `staging/` reais.
- A DAG passa no teste de DagBag.
- Falha explícita quando a fonte não devolve dado.
