# Issue #130 - Analise de Lacunas: Staging MinIO x Indicadores do Reloginho

> Data da varredura: 2026-08-25. Bucket `data-lake-mcid`, endpoint `10.0.0.56:9000`
> (leitura via `mc`). Objetivo: registrar o estado atual do `staging/` do MinIO em
> relacao ao que os indicadores da #130 precisam, e sinalizar as lacunas que os
> re-tratamentos de `dados_historicos` e `sftp` devem cobrir.
>
> **Atualizacao 2026-09-02:** o gold do reloginho (grupo A) passou a existir e foi
> refatorado em bronze -> silver -> gold
> (`bronze_reloginho_snh_serie_mensal` -> `silver_reloginho_snh_apf_mes` ->
> `indicadores_reloginho` / `indicadores_reloginho_frente` /
> `resumo_reloginho_dashboard`), lendo `staging/dados_historicos/*ecente_*` via
> DuckDB. Ver `docs/entregas/issue-130-refatoracao-medalhao-reloginho.md`. Os
> status "bloqueado" da secao 4 abaixo valem so para os indicadores dependentes de
> META OFICIAL; `uh_contratadas`, `uh_entregues` e `ritmo_medio_mensal` estao
> materializados.

## Resumo

- A arquitetura exige que os modelos dbt leiam **`staging/` do MinIO** (nunca `raw/`).
- As duas bases que alimentam o reloginho estao em re-tratamento e, hoje, **nao
  condizem** com o esperado:
  - `dados_historicos`: 754 CSVs em `raw/`, 746 parquets em `staging/` — alvo e
    **< 500 tabelas canonicas**.
  - `sftp`: 2.842 parquets em `staging/sftp/` — alvo e **1.453 tabelas canonicas**.
- O ponteiro "atual" do reloginho (SNH 202606) so existe em `raw/`, sem equivalente
  em `staging/` — a serie tratada vai ate 2026-03.
- O gold do reloginho (grupo A) le a serie mensal `historico_recente_*` de
  `staging/dados_historicos/` via DuckDB, nas camadas bronze/silver/gold (ver nota
  de atualizacao acima). Depende do `staging/dados_historicos` tratado.
- Os indicadores de gargalo (grupo B) nao dependem dessas bases (leem FAR/FDS do
  Postgres), portanto nao sao afetados pelos re-tratamentos.

---

## 1. Base `dados_historicos`

| Camada | Objetos | Alvo | Situacao |
|---|---:|---:|---|
| `raw/dados_historicos/` | 754 CSV | — | fonte bruta |
| `staging/dados_historicos/` | 746 parquet | < 500 canonicas | **desatualizado / divergente** |

Observacoes:

- Nomenclatura 1:1: `raw/<nome>.csv` -> `staging/dados_historicos/<nome>.parquet`.
  O staging atual usa sufixo `.parquet` (o inventario #66 registrava o padrao antigo
  `*.csv.parquet`, ja substituido).
- **8 arquivos presentes em `raw/` sem parquet correspondente em `staging/`:**

  | # | Arquivo (raw) |
  |---|---|
  | 1 | `bb_2013_06_junho_pmcmv_18062013_tab_andamento_obras` |
  | 2 | `bb_2013_06_junho_pmcmv_18062013_tab_arquivos_dados` |
  | 3 | `bb_2013_06_junho_pmcmv_18062013_tab_caracterizacoes_entornos` |
  | 4 | `bb_2013_06_junho_pmcmv_18062013_tab_contratos_pj` |
  | 5 | `bb_2013_06_junho_pmcmv_18062013_tab_proponentes` |
  | 6 | `bb_2013_06_junho_pmcmv_18062013_tab_unidades_concluidas` |
  | 7 | `bb_2015_03_marco_cgu_of_6263_tab_emp_20150831` |
  | 8 | `caixa_001_2016_bext_31102016` |

  Provavelmente falharam na conversao para parquet (tabelas multi-cabecalho/relacionadas
  do dump BB 2013, CGU e CAIXA BEXT).

- Arquivos com acentos e espacos no nome (`previsao_de_conclusao`,
  `relatorio_executivo`, `___copia`). A macro `minio_staging_uri()` monta
  `s3://bucket/staging/<object_name>` **sem URL-encoding** — referenciar esses objetos
  exige nome exato.

## 2. Base `sftp`

| Camada | Objetos | Alvo | Situacao |
|---|---:|---:|---|
| `staging/sftp/` | 2.842 parquet | 1.453 canonicas | **desatualizado / divergente** |

Quebra por diretorio:

| Diretorio | Parquet |
|---|---:|
| `caixa.geavo/GEAVO/` | 1.343 |
| `fabrica/GEFUS/ANTERIORES/` | 1.321 |
| `fabrica/GEFUS/` | 163 |
| `fabrica/Analise_SNH/` | 8 |
| `fabrica/GEFUS/ANTERIORES/TESTE_UNIFICACAO_RURAL/` | 4 |
| `fabrica/GEFUS/FDS/` | 3 |
| **Total** | **2.842** |

As frentes operacionais que leem `source("sftp_mcmv", ...)` (Classe Media/Faixa 3,
Reforma, Cidades, Rural/PNHR `int057`/`int065`) dependem deste staging estar reduzido
as tabelas canonicas.

## 3. Ponteiro atual SNH (lacuna adicional)

O snapshot "atual" do reloginho usa `raw/202606_SNH_PMCMV_DADOS_PRIORITARIOS_*`
(CAIXA e BB, contratadas + entregas). Nao existe prefixo correspondente em `staging/`
(folders atuais: `abecip, bacen, dados_historicos, fgv, fipe, ibge, infomoney,
novo_caged, sftp, sharepoint, siafi-tesouro-gerencial`). Pela regra "dbt so le
staging", essa base tambem precisa ser estagiada, senao o reloginho fica com hiato
entre o fim da serie tratada (2026-03) e o "agora".

## 4. Matriz indicador x fonte staging x status

### Grupo A — Reloginho (10)

| Indicador | Fonte atual nos docs | Staging esperado | Status |
|---|---|---|---|
| `uh_meta_total` | tabela oficial de metas | parametro/tabela de metas | pendente de negocio (nao e MinIO) |
| `uh_contratadas` | `historico_recente_*` (serie) + `raw/202606_SNH_*` (atual) | `staging/dados_historicos.historico_recente_*` + staging SNH (atual) | **materializado** para a serie 2024-06..2026-03 (`indicadores_reloginho` / `_frente`); ponteiro atual (SNH 202606) pendente |
| `uh_entregues` | `historico_recente_*` (acumulado) + `raw/202606_SNH_*_ENTREGAS` (atual) | `staging/dados_historicos.historico_recente_*` + staging SNH | **materializado** para a serie 2024-06..2026-03; ponteiro atual pendente |
| `perc_meta_contratada` | derivado | idem `uh_contratadas` + meta | bloqueado (meta) |
| `perc_meta_entregue` | derivado | idem `uh_entregues` + meta | bloqueado (meta) |
| `gap_uh_meta` | derivado | idem `uh_entregues` + meta | bloqueado (meta) |
| `ritmo_medio_mensal` | serie mensal entregas acumuladas | `staging/dados_historicos.historico_recente_*` | **materializado** (`resumo_reloginho_dashboard`); janela do denominador a confirmar (decisao #8) |
| `ritmo_necessario` | derivado | meta + serie entregas + fim ciclo | bloqueado (meta + fim de ciclo) |
| `projecao_entrega` | derivado | meta + serie entregas | bloqueado (meta + janela ritmo recente) |
| `status_relogio` | derivado | demais + faixas de corte | bloqueado (faixas + meta) |

### Grupo B — Gargalo/desempenho (9)

Origem: `mcmv_indicadores.indicadores_gargalo_desempenho` <- golds FAR/FDS do
Postgres (`__dados_brutos`). **Nao dependem** de `dados_historicos`/`sftp`. Bloqueio
unico e a materializacao (`dbt run`), que requer credencial Postgres.

## 5. Acao necessaria

1. **Re-tratamento `dados_historicos`** deve entregar em `staging/dados_historicos/`
   as **< 500 tabelas canonicas** (hoje 746), resolvendo tambem os 8 arquivos
   ausentes da conversao.
2. **Re-tratamento `sftp`** deve reduzir `staging/sftp/` de 2.842 para as **1.453
   tabelas canonicas**.
3. **Criar staging para a base SNH atual** (`202606_SNH_*`) — fora do escopo dos dois
   re-tratamentos, mas necessaria para o ponteiro atual do reloginho (sem ela a
   serie do reloginho termina em 2026-03).
4. ~~Construir a gold do reloginho (grupo A) lendo staging via DuckDB.~~ **FEITO**
   (2026-09-02) — bronze/silver/gold em `indicadores_mcmv_dbt/`, ver
   `docs/entregas/issue-130-refatoracao-medalhao-reloginho.md`. Resta reapontar o
   piloto #118 (`mcmv_historico`) do seed para `staging/dados_historicos`.
