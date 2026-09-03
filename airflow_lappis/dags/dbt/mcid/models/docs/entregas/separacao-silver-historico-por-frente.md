# Separação da silver histórica de empreendimentos por frente

Change OpenSpec: `separacao-silver-historico-por-frente`.
Objetivo: separar a série histórica mensal de empreendimentos MCMV por frente
(FAR, FDS/Entidades, Rural) na camada silver, criando a bronze fiel que faltava
entre `staging/` e a silver, e alinhar o eixo histórico ao padrão de
nomenclatura (prefixo de camada + token `mcmv_historico` + schemas globais).

## 1. De/para de nomes

| Papel | Antes | Depois | Schema (prod) |
|---|---|---|---|
| bronze fiel SFTP (5 interfaces) | — (não existia) | `bronze_mcmv_historico_empreendimento_sftp` | `bronze` |
| bronze fiel SNH (`historico_recente_*`) | dentro de `bronze_reloginho_snh_serie_mensal` | `bronze_mcmv_historico_empreendimento_snh` | `bronze` |
| silver série por frente | — (uma tabela só) | `silver_mcmv_historico_empreendimento_far` / `_fds` / `_rural` | `silver` |
| silver série consolidada | `historico_mcmv_empreendimentos_snapshot` | `silver_mcmv_historico_empreendimento` | `silver` |
| gold estado corrente | `snapshot_mcmv_empreendimentos_atual` | `gold_mcmv_snapshot_empreendimento_atual` | `gold` |
| bronze série executiva (pré-2024) | `bronze_mcmv_serie_executiva_historica` | `bronze_mcmv_historico_serie_executiva` | `bronze` |
| silver série executiva (pré-2024) | `silver_mcmv_serie_executiva_historica` | `silver_mcmv_historico_serie_executiva` | `silver` |
| gold série mensal agregada | `gold_mcmv_serie_historica_mensal` | `gold_mcmv_historico_serie_mensal` | `gold` |
| piloto série anual OGU/FGTS (#118) | `historico_mcmv_serie_temporal_snapshot` | `silver_mcmv_historico_serie_anual_ogu_fgts` | `silver` |

Regra lexical aplicada: série temporal ⇒ contém `historico` no nome; estado
corrente ⇒ contém `snapshot`. Por isso o piloto (série **anual**) perdeu o
`snapshot` do nome.

Pastas: `models/mcmv_historico_dbt/{bronze,silver,gold,piloto}/`. As pastas
`empreendimentos/` e `serie_executiva/` deixaram de existir.

Schema global: `macros/get_custom_schema.sql` passou a honrar o `+schema` custom
também no target `staging_duckdb` (antes só em `prod`). O ramo `prod` continua
delegando a `generate_schema_name_for_env` — sem efeito em conjuntura/legado.
Mesma decisão D4 da change `migracao-bronze-minio-mcmv` (esta entrou primeiro).

## 2. Mapa `modalidade` (SNH) → `frente_mcmv`

Levantado do MinIO real (`staging/dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_*.parquet`,
38 arquivos, 2024-06 → 2026-03, exclui `%entrega%`):

| `modalidade` na fonte | Agente (nome do arquivo) | Linhas | `frente_mcmv` |
|---|---|---:|---|
| `FAR` | CAIXA | 179.616 | FAR |
| `FAR` | BB | 6.800 | FAR |
| `Entidades` | CAIXA | 28.292 | Entidades |
| `RURAL` | CAIXA | 366.908 | Rural |
| `Rural` | BB | 36.992 | Rural |

Regra: `case upper(trim(modalidade)) when 'FAR' then 'FAR' when 'ENTIDADES' then
'Entidades' when 'RURAL' then 'Rural' end`.

**Open Question 2 (design) resolvida:** não há modalidade fora de FAR / Entidades
/ Rural. Nenhuma silver `_outras` criada; nenhuma linha SNH fica "Não
classificada".

## 3. Contrato semântico comum (silver por frente)

33 colunas: 25 de negócio + 8 técnicas.

Negócio: `programa`, `frente_mcmv`, `grupo_linha`, `linha_mcmv`, `grao_registro`,
`agente_financeiro`, `apf`, `codigo_empreendimento`, `nome_empreendimento`,
`codigo_ibge_municipio`, `municipio`, `uf`, `responsavel_id`, `responsavel_nome`,
`quantidade_uh`, `quantidade_uh_entregues`, `valor_contratado`,
`valor_desembolsado`, `percentual_execucao_fisica`, `status_operacional`,
`dt_contratacao`, `dt_inicio_obra`, `dt_entrega`, `dt_referencia`, `dt_movimento`.

Técnicas: `id_historico_snapshot` (`md5(frente|apf|dt_referencia)`, único),
`id_negocio_historico` (`md5(programa|frente|apf)`, estável entre meses),
`fonte_serie` (`sftp` | `snh`), `fonte_tabela`, `source_file`, `hash_linha`,
`dt_ingest`, `dt_silver`.

Discriminador de frente:
- SFTP: interface de origem — INT040/INT054 → FAR; INT059 → Entidades;
  INT057/INT065 → Rural.
- SNH: coluna `modalidade` (mapa da seção 2).

Tipagem: `parse_hist_double` / `parse_hist_bigint` / `parse_hist_date` (absorvem
formato brasileiro `13.898.046,25`, dot-decimal e `None`/`nan`). O `exec` do SNH
(`97,2`) vira `percentual_execucao_fisica`.

## 4. Precedência SFTP × SNH (janela 2024-06 → 2024-11)

Quando `(frente_mcmv, apf, dt_referencia)` existe nas duas fontes, prevalece o
**SNH** (`row_number()` ordenando `snh` antes de `sftp`). Antes do `row_number()`,
as colunas presentes só no SFTP (`dt_inicio_obra`, `responsavel_id`,
`responsavel_nome`, `dt_movimento`; para FDS também `quantidade_uh_entregues`)
são propagadas dentro do grão via `max() over (partition by frente, apf,
dt_referencia)` e recuperadas por `coalesce` na projeção final — a linha
vencedora do SNH não perde esses campos.

## 5. Reloginho

`bronze_reloginho_snh_serie_mensal` passou a ser
`select * from {{ ref('bronze_mcmv_historico_empreendimento_snh') }}`. A bronze
compartilhada usa a **mesma** glob, o mesmo filtro `not like '%entrega%'` e a
mesma expressão de `hash_linha` de antes, além de derivar `agente_arquivo`,
`prioridade_reentrega`, `dt_referencia`, `dt_ingest`, `source_file`. Contrato de
saída idêntico → `silver_reloginho_snh_apf_mes` e os golds
(`indicadores_reloginho*`) não mudam. Os fluxos de entrega por evento continuam
em `bronze_reloginho_snh_entregas_evento`.

## 6. Consumidores / blast radius

- Consumidores das tabelas renomeadas/removidas dentro do repo dbt: apenas
  `tests/mcmv_historico/*` (5 testes, repontados) e `dbt_project.yml`.
- Nenhum card do Superset consome essas tabelas — **falta confirmar no
  repositório de dashboards do Superset** (inventário #119) antes do merge.
- Pipeline de produção (Cosmos, `target=prod`): bronze/silver/gold de
  `mcmv_historico_dbt` nascem `+enabled: duckdb` → fora da carga noturna até o
  ADR #117 (mesma situação do reloginho e `mcmv_silver_dbt`). O `piloto` (seed,
  Postgres) continua rodando no `prod`, agora no schema `silver`.

## 7. Validação executada (dbt-core 1.11.14 + dbt-duckdb 1.11 contra o MinIO real)

| Passo | Resultado |
|---|---|
| `dbt parse --target prod` · `dbt parse` + `compile --target staging_duckdb` | OK, 0 erros (109 models) |
| `dbt ls --target prod` | bronze/silver/gold de empreendimento e série executiva `disabled`; só o piloto (`silver_mcmv_historico_serie_anual_ogu_fgts`) enabled @ `silver`. conjuntura / far / reloginho **sem mudança de schema** |
| `dbt build` bronze SFTP + SNH | **OK** — SNH 28s, SFTP 90s, PASS=7 |
| `dbt build` silver `_far`/`_fds`/`_rural`/consolidado + gold snapshot + `dbt test` | **OK** — PASS=53, 0 erros |
| testes: `unique`/`not_null` de `id_historico_snapshot`, `accepted_values` de `frente_mcmv` e `fonte_serie`, `assert_empreendimentos_dt_movimento_consistente` (repontado) | todos PASS |
| piloto: `dbt seed issue_118` (INSERT 17) + build `silver_mcmv_historico_serie_anual_ogu_fgts` + 18 testes | **OK** |
| reloginho D8 (baseline com glob antigo vs. `ref()` da bronze compartilhada) | `assert_reloginho_reconcilia_66` **PASS** nos dois · `grain_unique` PASS · `cobertura_mensal` FAIL 5 **idêntico** nos dois (2024-08 ausente do dump SNH — pré-existente, **não é regressão**) |
| `dbt docs generate --target staging_duckdb` | `catalog.json` + `manifest.json` gerados |

Contagem de linhas (silver):

| Frente | `sftp` | `snh` | Total | Janela |
|---|---:|---:|---:|---|
| FAR | 250.254 | 92.723 | 342.977 | sftp 2019-12→2024-11 · snh 2024-06→2026-03 |
| Entidades | 39.296 | 14.146 | 53.442 | sftp 2019-12→2026-06 · snh 2024-06→2026-03 |
| Rural | 535.968 | 200.862 | 736.830 | sftp 2019-12→2024-11 · snh 2024-06→2026-03 |
| **consolidado** | | | **1.133.249** | |
| `gold_mcmv_snapshot_empreendimento_atual` | | | 17.656 | 1 linha por (frente, APF) |

Os totais `sftp` (250.254 / 39.296 / 535.968 = 825.518) batem **1:1** com o
modelo `historico_mcmv_empreendimentos_snapshot` anterior — o eixo SFTP migrou
sem perda; as linhas SNH são história adicional (2024-06+). Na janela sobreposta
2024-06→2024-11, a precedência SNH mantém 1 linha por grão (`unique` passou).

## 7a. Guardas de recurso para o build local

`profiles.yml` (`staging_duckdb.settings`) ganhou guardas overridáveis por env
var: `preserve_insertion_order: false` (a materialização default buffra o
resultado inteiro em RAM — as bronzes são append-only, dedup por chave na
silver), `memory_limit`, `temp_directory`, `max_temp_directory_size`, e
`threads` templatizado. Sem env var, o comportamento é o de antes + spill em
disco limitado.

Helper: `run-historico.sh` (na raiz do projeto dbt) — carrega os `.env`, aponta
o DuckDB para `/mnt/data` (disco com espaço), limita RAM/threads, e builda
`seed → bronzes → silvers → golds → testes` **um modelo por vez**. Usa o
`dbt-core` do `.venv` do repo (o `dbt-fusion` do PATH não parseia este repo).

## 7b. Pendências de execução

1. **Série executiva** (`bronze_mcmv_historico_serie_executiva` → silver → gold):
   só renome, lógica byte-idêntica, **mas não conclui na máquina de dev** — o
   `union_by_name` das 4 famílias + `row_number() over (partition by source_file)`
   sobre a tabela ultra-larga derrama >40 GB e roda >20 min sem terminar. Defeito
   de escalabilidade **pré-existente** do modelo. Rodar no servidor/CI, ou abrir
   change separada de perf (sem mexer na lógica — é track paralela).
2. `dbt compile --target prod` — `local.env` só tem placeholders de
   `DB_DW_*_MCID`; rodar no deploy (Airflow injeta as credenciais).
3. Deploy prod: dropar `mcmv_historico.historico_mcmv_serie_temporal_snapshot`
   (órfão — o piloto passou a materializar em `silver`).
4. Confirmar ausência de consumidor no repositório de dashboards do Superset
   (dentro do repo dbt: só `tests/mcmv_historico/*` e `dbt_project.yml`).
3. Reloginho: `dbt build --select +indicadores_reloginho +indicadores_reloginho_frente
   +indicadores_reloginho_entregas` antes/depois — `row_count` igual e
   reconciliação #66 dentro de ±0,5 %. (Mudança em 7.2 é pass-through: mesma
   glob, mesmo filtro `%entrega%`, mesma expressão de `hash_linha`.)
4. `dbt compile --target prod` + `dbt ls --target prod` — bronze/silver/gold de
   `mcmv_historico_dbt` aparecem `disabled`; conjuntura/legado não mudam de
   schema (a macro só altera o ramo `target.type == 'duckdb'`).
5. `dbt docs generate`.
6. Confirmar ausência de consumidor no repositório de dashboards do Superset.

## 8. Fase 2 (change separada — `historico-empreendimento-gap-fillers`)

As silvers por frente já nascem com CTEs nomeadas por fonte e comentários
`-- fase 2: <bloco>` nos pontos de extensão. A fase 2 acrescenta, de forma
aditiva (novos CTEs + `union all` + mapa de colunas), os blocos ainda parados em
`staging/dados_historicos`: gap-fillers BB 2015-2019, CAIXA PF 2010-2013,
INT0xx dentro do dump (2018), PNHR BB 2014-2018, PMCMV-3/Faixa 3, agregados
executivos para reconciliação, TransfereGov/FNHIS. Detalhe na seção "Fase 2" do
`design.md` da change.
