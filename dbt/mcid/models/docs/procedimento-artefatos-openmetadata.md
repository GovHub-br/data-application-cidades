# Procedimento — artefatos do dbt para o OpenMetadata

## 1. O que o conector lê

O conector dbt do OpenMetadata ingere **três artefatos** do dbt:

| artefato | gerado por | leva ao OpenMetadata |
|---|---|---|
| `manifest.json` | `dbt docs generate` (ou `dbt parse`) | nós, `description`, `tags`, `meta` (`governance` + `openmetadata`), `depends_on` (**linhagem**) |
| `catalog.json` | `dbt docs generate` (**introspecta o banco**) | tipo de cada coluna, contagem de linhas |
| `run_results.json` | o último comando do dbt no mesmo `target/` | status por nó |

Só o `manifest.json` é obrigatório. Sem os outros dois a ingestão segue, mas o
catálogo fica sem os tipos e sem as estatísticas de execução.

O FQN de cada nó é `<database>.<schema>.<name>`. O `dbt_project.yml` fixa
`+database: cidades`, e o `macros/get_custom_schema.sql` dá o mesmo schema em
`prod` e no `staging_duckdb`, então o FQN é `cidades.<bronze|prata|ouro|...>.<tabela>`
nos dois.

## 2. Como são gerados — automático

Quem gera é a task `dbt_metadata` da `openmetadata_ingestion_dag`
(`dags/snh/openmetadata_ingestion_dag.py`), por `helpers/openmetadata/dbt_artifacts.py`:

1. copia `dbt/mcid/` para um diretório temporário, sem `target/`, `logs/` e
   `dbt_packages/`;
2. roda `dbt deps` e `dbt docs generate --selector dominios_postgres` no target
   padrão do `profiles.yml` (`prod`). O seletor é o mesmo do `mcid_cosmos_dag`:
   o projeto inteiro, com as fontes;
3. aponta a recipe `helpers/openmetadata/recipes/dbt_metadata.yaml` para o `target/`
   dessa cópia.

Consequências:

- O `run_results.json` que chega ao OpenMetadata é o do `docs generate`, não o
  dos `dbt test` do Cosmos. **O status dos testes da execução diária não vai para
  o catálogo.** Para ver o que falhou, consulte `lake._dbt_log`: o hook `on-run-end`
  grava ali o que não passou em cada execução no Postgres.
- O `catalog.json` introspecta o banco no momento da ingestão: a tabela aparece com
  os tipos da última materialização bem-sucedida.
- A `dbt_metadata` só **anexa** descrição, tier, domínio e linhagem a tabelas que a
  `postgres_metadata` já criou no catálogo (schemas `bronze`, `prata`, `ouro` e
  `metadata`). A ordem das recipes está em `RECIPE_PIPELINE`, em
  `helpers/openmetadata/config.py`.

A `openmetadata_ingestion_dag` roda todo dia e também é disparada pelo
`mcid_cosmos_dag` ao final, mesmo quando algum model ou teste falha
(`trigger_rule="all_done"`).

## 3. Gerar à mão, para inspecionar

```bash
cd dbt/mcid
uv run --env-file ../../.env dbt docs generate --selector dominios_postgres
```

Os arquivos saem em `dbt/mcid/target/`. O `catalog.json` precisa de conexão com o
banco do `profiles.yml`.

**Não publique esse `target/`.** O manifest e o catalog completos incluem a bronze e
o SQL compilado. A única forma aprovada de publicar a documentação do dbt é
`make conjuntura-docs`, que publica só a projeção permitida.

## Histórico

Até 11/2026 este documento descrevia a geração manual dos artefatos do antigo eixo
histórico (`mcmv_historico_dbt` e `indicadores_mcmv_dbt`), num DuckDB local, quando
ainda não havia ingestão automática. O eixo saiu do repositório no #307 e foi para o
`analytics_mcid`, em `legado/dbt/`; a versão anterior deste arquivo está no histórico
do git.
