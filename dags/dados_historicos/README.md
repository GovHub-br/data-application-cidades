# Dados históricos do MCMV

`dados_historicos_dag.py` reconstrói a série histórica do MCMV a partir do dump congelado
do banco antigo (754 CSVs em `raw/dados_historicos/`, subidos à mão). Todo o código está no
módulo da DAG; não há scripts nem plugin próprio.

## Camadas (todas em `tests/` enquanto a validação não termina)

| Task | Grava em | O que faz |
|---|---|---|
| `conferir_raw` | — | falha se sumiu da raw algum arquivo já convertido (upload pela metade) |
| `raw_para_staging` | `tests/staging/dados_historicos/` | CSV → Parquet um a um, tudo texto; mascara dado pessoal |
| `staging_para_analytics` | `tests/analytics/dados_historicos/` | tira vazios, o modelo de 2009 e cópias exatas |
| `analytics_para_cleansed` | `tests/cleansed/dados_historicos/<familia>/` | estrutura (2×, cabeçalho, descartes), competência e corte, padronização de formato, duplicatas e versões |
| `mapear_colunas` | `tests/cleansed/dados_historicos/_colunas_*` | evidência do grafo de nomes de coluna |

Cada camada é um Asset (`dados_historicos_raw` → `_staging` → `_analytics` → `_cleansed`).
A DAG dispara por um evento do Asset da raw (criado na UI por quem sobe os arquivos) ou à mão.
Params: `forcar` (reconverte a staging) e `filtro` (regex sobre o nome do arquivo).

Controle no cleansed: `_cleansed_log.parquet` (uma linha por saída), `_cleansed_descartes.parquet`
(cada linha tirada, com o motivo), `_cleansed_padronizacoes.parquet` (células alteradas por
coluna e regra) e `_versoes_substituidas/` (versões trocadas, fora da pasta que a bronze lê).

## Configuração

- Conexão `minio_lake` (tipo `aws`, com o `endpoint_url` do MinIO no extra).
- Os mapas de tratamento **não ficam no repositório**: estão no MinIO, em
  `tests/config/dados_historicos/` (`cabecalhos.csv`, `familias.csv`, `de_para_colunas.csv`).

## Rodar localmente

```bash
export AIRFLOW_HOME=$(mktemp -d) AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags/dados_historicos AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW_CONN_MINIO_LAKE='{"conn_type": "aws", "login": "<access>", "password": "<secret>", "extra": {"endpoint_url": "http://<minio>:9000"}}'
uv run airflow db migrate
uv run airflow dags test dados_historicos_dag            # ou: airflow tasks test dados_historicos_dag <task>
```

Testes: `uv run pytest tests/test_dados_historicos_dag.py` (lake em memória, sem MinIO).

## Notebooks (`notebooks/dados_historicos/`)

`00` visão geral · `01`/`02` EDA da entrada BB · `03` grafo de nomes de coluna · `04` estudo
das colunas por entidade e dicionário proposto · `05` conferência de que a padronização não
perdeu informação (compara com `tests/cleansed_antes_padronizacao/`).

## Estado e próximos passos (16/11/2026)

- Cleansed: 434 saídas em 54 famílias + 16 versões substituídas, 14.776.048 linhas; conferência
  independente com 0 células não equivalentes.
- A validar: as questões da seção 8 do notebook 04 e o de-para (`status = proposto`); conteúdo
  idêntico em meses diferentes hoje vira versão substituída; as bases 5c e 5c1 de 2012-04 ficaram
  as duas.
- Próximo: medalhão no dbt — bronze por família lendo o cleansed; silver por família com o de-para
  (lido do MinIO), tipagem e escalas; gold com as fatos e dimensões (primeira fatia: a cadeia do
  APF da Caixa). Formato fica na DAG; significado, no dbt.
