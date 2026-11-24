# Dados históricos do MCMV

`dados_historicos_dag.py` reconstrói a série histórica do MCMV a partir do dump congelado
do banco antigo (754 CSVs em `raw/dados_historicos/`, subidos à mão).

## Onde está o código

- **DAG** (`dados_historicos_dag.py`): só orquestração e I/O. Lê e grava no lake pelo `S3Hook`
  (provider amazon) e encadeia as tasks.
- **Tratamento** (`helpers/historico_mcmv/`): funções puras, que recebem DataFrame ou caminho
  local e não sabem de S3. Há um módulo por etapa:
  - `staging`: CSV → Parquet e máscara de dado pessoal;
  - `analytics`: métricas e seleção;
  - `estrutura`: duplicação 2×, cabeçalho, descartes e conservação;
  - `padronizacao`;
  - `datas`: competência e corte;
  - `cleansed`: plano, famílias, log e versões;
  - `colunas`: grafo de nomes;
  - `nomes`: nome de coluna normalizado;
  - `layout`: os prefixos do lake.
- **Validação** (`helpers/historico_mcmv/validacao.py`, pandera): o contrato de cada fronteira.
  - Na entrada: os mapas de configuração, validados assim que são lidos.
  - No controle: o manifesto, os logs e os descartes, validados antes de subir.
  - Na saída: cada arquivo do cleansed (`_linha_origem` inteiro e único, o resto texto e com
    nome normalizado).
  - Uma falha para a task com os casos que quebraram o contrato.
- A normalização de nomes de coluna e a correção de mojibake (`nomes.py`) seguem a mesma regra de
  `scripts/lake_utils.py`. Um teste confere a equivalência, e o código não importa de `scripts/`.

## Camadas (todas sob `staging/historico_mcmv/`)

| Task | Grava em | O que faz |
|---|---|---|
| `conferir_raw` | — | falha se sumiu da raw algum arquivo já convertido (upload pela metade) |
| `raw_para_staging` | `staging/historico_mcmv/convertido/` | CSV → Parquet um a um, tudo texto; mascara dado pessoal |
| `staging_para_analytics` | `staging/historico_mcmv/analytics/` | tira vazios, o modelo de 2009 e cópias exatas |
| `analytics_para_cleansed` | `staging/historico_mcmv/cleansed/<familia>/` | estrutura (2×, cabeçalho, descartes), competência e corte, padronização de formato, duplicatas e versões |
| `mapear_colunas` | `staging/historico_mcmv/cleansed/_colunas_*` | evidência do grafo de nomes de coluna |

Cada camada é um Asset (`dados_historicos_raw` → `_staging` → `_analytics` → `_cleansed`).
A DAG dispara por um evento do Asset da raw (criado na UI por quem sobe os arquivos) ou à mão.
Params: `forcar` (reconverte a staging) e `filtro` (regex sobre o nome do arquivo).
Arquivos da raw em `RAW_FORA` nunca entram no pipeline (hoje, a `loginfesta` do BB: credenciais, sem
dado do programa).

Controle no cleansed: `_cleansed_log.parquet` (uma linha por saída), `_cleansed_descartes.parquet`
(cada linha tirada, com o motivo), `_cleansed_padronizacoes.parquet` (células alteradas por
coluna e regra) e `_versoes_substituidas/` (versões trocadas, fora da pasta que a bronze lê).

## Configuração

- Conexão `minio_lake` (tipo `aws`, com o `endpoint_url` do MinIO no extra).
- Os mapas de tratamento **não ficam no repositório**: estão no MinIO, em
  `staging/historico_mcmv/config/` (`cabecalhos.csv`, `familias.csv`, `de_para_colunas.csv`).
  No `cabecalhos.csv`, por arquivo:
  - a ação do cabeçalho;
  - os blocos;
  - `linhas_zeradas` (descarta as linhas-modelo zeradas);
  - `linhas_total` (`_linha_origem` de um total sem rótulo; sai como `total`).

## Rodar localmente

```bash
export AIRFLOW_HOME=$(mktemp -d) AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags/dados_historicos AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW_CONN_MINIO_LAKE='{"conn_type": "aws", "login": "<access>", "password": "<secret>", "extra": {"endpoint_url": "http://<minio>:9000"}}'
uv run airflow db migrate
uv run airflow dags test dados_historicos_dag            # ou: airflow tasks test dados_historicos_dag <task>
```

Testes: `uv run pytest tests/test_historico_mcmv.py tests/test_dados_historicos_dag.py` (helpers puros; a DAG com um S3Hook em memória, sem MinIO).

## Medalhão no dbt (`dbt/mcid/models/dados_historicos_dbt/`)

A DAG cuida do formato; o significado fica no dbt.

- **Bronze**: `bronze_dhist_<familia>`, uma por família (53). É cópia fiel do cleansed, com
  `familia`, `saida` e `source_file`. Mais `bronze_dhist_cleansed_log`.
- **Silver**: `prata_dhist_<familia>`, com o de-para de `macros/dados_historicos/de_para.sql`
  (nome canônico ← colunas de origem, e o tipo). Traz a competência e o corte do log, o IBGE
  de 7 dígitos e a situação canônica. São 46 famílias; as 7 sem valor para a gold ficam em
  `descartes_dhist()`, com o motivo. O BB ganha o APF pela ponte do INT054
  (`prata_dhist_ponte_bb_apf`).
- **Execução**: no target do banco, dentro do grupo Postgres da `mcid_cosmos_dag`. O SQL é do
  DuckDB e roda no pg_duckdb, dentro do envelope `duckdb_query()`; dentro dele, as tabelas do
  Postgres são lidas como `pgduckdb.<schema>.<tabela>` (`rel_pgduckdb()`). As bronzes leem o
  cleansed pelo `fonte_lake` (fonte `lake_staging_historico` em `sources.yml`).
- **Testes** (`dbt/mcid/tests/dados_historicos/`):
  - linhas por saída e colunas da bronze contra o log;
  - linhas da silver contra a bronze;
  - tipagem sem perda;
  - cobertura do de-para;
  - situação no domínio;
  - formato do APF;
  - cobertura das famílias (silver ou descarte);
  - ponte BB → APF com chave única;
  - percentual em 0–100;
  - dado pessoal só mascarado.

Ordem na homologação: a `dados_historicos_dag` grava o cleansed; depois a `mcid_cosmos_dag`
constrói bronze, silver e testes.

## Notebooks (no repositório `analytics_mcid`, em `notebooks/historico/`)

- `00`: visão geral.
- `01`/`02`: EDA da entrada BB.
- `03`: grafo de nomes de coluna.
- `04`: estudo das colunas por entidade e dicionário proposto.
- `05`: conferência de que a padronização não perdeu informação (compara com
  `tests/cleansed_antes_padronizacao/`).
- `06`: validação da silver da cadeia do APF da Caixa. Lê o DuckDB local.
- `07`: validação da silver do BB, da contratação PF e das demais famílias (só contagens).

## Estado e próximos passos (16/11/2026)

- Cleansed: 433 saídas em 53 famílias + 16 versões substituídas, 14.775.919 linhas; conferência
  independente com 0 células não equivalentes.
- A validar: as questões da seção 8 do notebook 04 e o de-para (`status = proposto`); conteúdo
  idêntico em meses diferentes hoje vira versão substituída; as bases 5c e 5c1 de 2012-04 ficaram
  as duas.
- Bronze das 53 famílias pronta e conferida contra o log. Silver de 46 famílias e 7 descartes,
  com todos os testes verdes (notebooks 06 e 07).
- Próximo: gold com as fatos e dimensões.
