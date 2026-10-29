# F3 · Refatoração do dbt: problemas do OpenMetadata, seletores, prod e homolog

> Parte do [guia das refatorações](README.md) · Frente prevista · Status: issue aberta · Issue: [#245](https://github.com/GovHub-br/data-application-cidades/issues/245) · PR: —

## Objetivo

Levar o `dbt/mcid` a um padrão único. Isso corrige os problemas que o OpenMetadata expôs e cria uma separação real entre prod e homolog.

## Já decidido

- **Ponte já entregue:** o PR #241, em que `selectors.yml` com `eixo_historico` e `dominios_postgres` permite que o `docs generate` da DAG do OM rode.
- **Ordem:**
  1. a DAG do OM roda verde no homolog;
  2. depois, um PR por domínio, por issue, após os [ADRs](f1-adrs.md).
- **Padrão-alvo proposto (a fechar):**
  - bronze só `select * from fonte_lake()`, com `union_by_name`;
  - prata e ouro em SQL de Postgres, com `coalesce` explícito, sem `describe` nem `run_query` na compilação;
  - um target só.
- **O que sai:** `prod_duckdb`, `staging_duckdb`, o pool `duckdb_historico`, os seletores e os shells `run-*.sh`.
- **Ordem dos domínios:**
  1. `linha_financiada_dbt` (quase pronto);
  2. `reforma_casa_brasil_dbt`;
  3. `mcmv_historico_dbt` (26 modelos, o mais pesado).
- **Antes de migrar o histórico:** definir `max_temp_directory_size` e `threads` do pg_duckdb 1.2.0. Hoje estão vazio e `-1`, e o piloto derrubou o prod em 05/09.

## Problemas encontrados

### Orquestração
- **✔ O reloginho lê o bronze da execução anterior.**
  - `indicadores_mcmv_dbt` está no grupo `dominios_postgres`, mas faz `ref(f.modelo)` nos `bronze_dhist_*` do `eixo_historico`, que roda depois (`dags/dbt/mcid_cosmos_dag.py:127`). Num banco zerado, quebra.
  - Também usa `try_cast` do DuckDB no target `prod` sem `force_execution`. Conferir.
- **O conjuntura roda duas vezes:** no `conjuntura_dag` (`select=["conjuntura_dbt"]`) e no `mcid_cosmos_dag`, cujo seletor cobre o projeto todo.

### OpenMetadata e catálogo
- **O eixo histórico não tem catálogo.** `helpers/openmetadata/dbt_artifacts.py:18` roda `docs generate --selector dominios_postgres`, e por isso mcmv_historico, reforma e linha_financiada ficam sem catálogo.
- **O docs da CI quebra.** `scripts/conjuntura/gerar_docs_seguros.py:81-90` roda sem seletor e falha no `describe`.
- **Linhagem cortada no bronze.** far, fds e rural (24 bronzes) leem `s3://data-lake-mcid/...` literal, sem `source()`.
- **Bucket configurado de três jeitos:** `var('lake_bucket')`, `env_var('MINIO_BUCKET')` e literal.

### Prod e homolog
- **Não existe target de homolog.** Homolog e prod rodam `prod` e só mudam as env vars.
- **O roteamento de schema não aguenta um target novo.** `macros/get_custom_schema.sql:19` só roteia `prod`, `prod_duckdb` e `staging_duckdb`. Um target `homolog` ingênuo jogaria tudo num schema só.

### Padrões e código morto
- **Cinco padrões de bronze.**
- **Ingestão duplicada.** INT057, INT059, INT065 e MONIT_MOV_OBRA alimentam duas cadeias: a do histórico e a de far, fds e rural.
- **Código morto:**
  - `models/mcmv_silver_dbt/`: 10 modelos desabilitados, com refs inexistentes;
  - `mcmv_historico_dbt/piloto/`;
  - 13 sources Postgres sem uso;
  - `analysis-paths` apontando para uma pasta que não existe;
  - `dbt deps` na CI sem `packages.yml`.
- **Falta de padronização:**
  - `config(materialized=...)` inline nos 278 modelos;
  - `tests:` e `data_tests:` misturados;
  - 21 snapshots com `target_schema='ouro'` fixo;
  - 16 colunas do boletim com espaço e acento.
- **SQL duplicado:**
  - `prata_rural_prioritarios_bb` e `_caixa` são idênticos;
  - os 14 `prata_conjuntura_ibge_*` são passthroughs que cabem numa macro;
  - os modelos de far e fds têm 70–80% de semelhança.

### Qualidade e documentação
- **Testes:** 104 modelos sem teste em YAML e cerca de 200 sem teste de unicidade.
- **Frescor:** nenhuma source tem `freshness`.
- **Sem entrada no YAML:** `ouro_conjuntura_boletim_p6_ogu` e `ouro_linha_financiada_mapa_uf`.
- **Docs fora do catálogo:** os 36 `.md` de `models/docs/` não são blocos `{% docs %}`, então não chegam ao catálogo.

## Depende de

- A DAG do OM verde no homolog.
- [F1](f1-adrs.md): nomenclatura.
- [B6](b06-ci-blocante.md): `dbt parse` na CI.

## Critério de aceite (por domínio)

- O domínio compila sem `describe` nem `run_query`.
- Aparece no catálogo do OM com a linhagem desde a source.
- Tem os testes mínimos de `unique` e `not_null` na chave.
