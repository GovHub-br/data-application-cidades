# Issue #130 — Implementacao dos Modelos Historicos de Empreendimentos (SFTP)

## Resumo

Foi implementada a primeira versao dos modelos de dados historicos de
empreendimentos MCMV, lidos diretamente do MinIO `staging/` via DuckDB, cobrindo
as frentes FAR, Entidades/FDS e Rural/PNHR. A entrega inclui:

- `historico_mcmv_empreendimentos_snapshot` — serie mensal (grao empreendimento
  x mes);
- `snapshot_mcmv_empreendimentos_atual` — estado corrente derivado do historico
  (decisao D2);
- macro de leitura da serie de parquets (`read_minio_staging_parquet_series`);
- testes dbt (`schema.yml` + teste singular de consistencia temporal).

## Contexto e Decisoes

- **#118** definiu a estrategia de dados historicos (snapshot, auditoria,
  retencao). O piloto usava seed CSV; esta entrega substitui o seed por fonte
  real do MinIO.
- **#130** valida os indicadores do "reloginho", que precisam da serie historica
  de empreendimentos por frente.
- **D1 (opcao B)**: manter `novo_mcmv_*` (SharePoint) e SFTP, reconciliar. Analise
  registrada em `issue-130-d1-reconciliacao-novo-mcmv-far.md`.
- **D2**: o snapshot corrente e derivado do historico (nao mantido separadamente).
- **Chave temporal**: `dt_referencia` extraida da data do **nome do arquivo**
  (mais confiavel que `dt_movimento`); `dt_movimento` fica como campo auxiliar.

## Arquivos criados/alterados

Criados:

- `macros/read_minio_staging_parquet_series.sql`
- `models/mcmv_historico_dbt/empreendimentos/historico_mcmv_empreendimentos_snapshot.sql`
- `models/mcmv_historico_dbt/empreendimentos/snapshot_mcmv_empreendimentos_atual.sql`
- `models/mcmv_historico_dbt/empreendimentos/schema.yml`
- `tests/mcmv_historico/assert_empreendimentos_dt_movimento_consistente.sql`
- `models/docs/entregas/issue-130-pendencias-encoding-canonicalizacao-sftp-minio.md`
- `models/docs/entregas/issue-130-d1-reconciliacao-novo-mcmv-far.md`

Alterados:

- `dbt_project.yml` — subpasta `mcmv_historico_dbt.empreendimentos` com
  `+enabled: "{{ target.type == 'duckdb' }}"`.
- `models/docs/entregas/README.md` — entradas dos novos documentos.

Dados (subprojeto `data-science/dados-historicos-tratamento`):

- `data/sftp_tratado/table_samples/` — 1.448 tabelas canonicas copiadas de
  `sftp/table_samples` (conforme `_canonicas.csv`), completando a base tratada.

## Modelo Historico

### Fontes (MinIO `staging/sftp/fabrica/GEFUS/`)

| CTE | Interface | Frente | Coluna temporal |
|---|---|---|---|
| `far_caixa` | INT040 | FAR CAIXA | `dt_movimento` |
| `far_bb` | INT054 | FAR BB | `dt_movimento` |
| `fds` | INT059 | Entidades/FDS | `dt_movimento` |
| `rural_bb` | INT057 | Rural PNHR BB | `coalesce(idt_movimento, dt_movimento)` |
| `rural_caixa` | INT065 | Rural PNHR CAIXA | `dt_movimento` |

### Grao e chave

- Grao: `empreendimento_mes` (uma linha por frente + APF + mes).
- `id_historico_snapshot = md5('empreendimento', frente_mcmv, apf, dt_referencia)`.
- Contrato comum: `programa, frente_mcmv, grupo_linha, linha_mcmv, grao_registro,
  agente_financeiro, apf, codigo_empreendimento, nome_empreendimento,
  codigo_ibge_municipio, municipio, uf, responsavel_id, responsavel_nome,
  quantidade_uh, quantidade_uh_entregues, valor_contratado, valor_desembolsado,
  percentual_execucao_fisica, status_operacional, dt_contratacao, dt_inicio_obra,
  dt_entrega, dt_referencia, dt_movimento, fonte_tabela, source_file, dt_silver`.

### Tratamentos aplicados

- Numericos em formato brasileiro (`13.898.046,25`) -> remove `.` de milhar e
  troca `,` por `.` antes do `try_cast`.
- Datas ISO -> `try_cast(... as date)`; vazios -> `nullif(trim(...), '')`.
- Exclusao de reentrega (`_0000`, `_V2`) via
  `regexp_matches(filename, '_\d{8}\.parquet$')`.
- Exclusao de arquivos `VALIDACAO` via `filename not ilike '%validacao%'`.
- Deduplicacao do grao via `row_number()` (algumas fontes trazem APF repetido no
  mesmo snapshot, ex.: FDS/INT059).

## Snapshot Corrente (D2)

`snapshot_mcmv_empreendimentos_atual` deriva do historico o ultimo mes por
(frente, APF), com `grao_registro = 'empreendimento'`. Permite, depois, decidir
entre manter so o historico ou tambem o snapshot.

## Validacao

### DuckDB (logica do SQL)

| Frente | Linhas | Duplicatas | Cobertura |
|---|---|---|---|
| FAR | 250.254 | 0 | 2019-12 .. 2024-11 |
| Entidades/FDS | 39.296 | 0 | 2019-12 .. 2026-06 |
| Rural | 535.968 | 0 | 2019-12 .. 2024-11 |

Total: 825.518 linhas, `id_historico_snapshot` unico.

### dbt

- `dbt parse`/`dbt compile --target staging_duckdb`: **0 erros nos arquivos
  criados** (2 modelos + macro + teste + schema).
- Ha 52 erros + 4 warnings **pre-existentes** em outros `schema.yml`/macros, por
  o `dbt-fusion 2.0.0-preview.212` ser mais estrito que a versao original do
  projeto. Tratamento da versao do dbt ficou fora desta entrega.

## Achados de qualidade de dados

1. **Reentrega** (`_0000`, `_V2`) e **VALIDACAO** duplicam o mesmo snapshot ->
   excluidos (canonicalizacao definitiva pendente, P2).
2. **INT057** tem a coluna temporal com nome inconsistente (`idt_movimento` vs
   `dt_movimento`) -> `coalesce`.
3. **INT057.qt_unidades** vazio em algumas linhas -> `quantidade_uh` NULL.
4. **INT065** esta no grao empreendimento (1:1 APF, media 23 UH, "carta de
   credito individual" e apenas 0,06%) -> sem ajuste necessario. Tem ~8x mais
   empreendimentos PNHR que o INT057 (CAIXA domina o PNHR).
5. **Typos de coluna** mapeados como estao (`sg_uf_muncicipio`, `no_empreeendmento`).

## Pendencias (proximos passos)

- **P1 (encoding)** — corrigir mojibake do `staging/` (tabelas SNH dados
  prioritarios).
- **P2 (canonicalizacao)** — dedup definitivo via `_canonicas.csv` no lugar dos
  filtros ad-hoc.
- **D1 (reconciliacao)** — implementar o CTE `novo_mcmv_far` no snapshot corrente
  (mapeamento em `issue-130-d1-reconciliacao-novo-mcmv-far.md`).
- **dbt** — tratar a versao do dbt para compilacao limpa do projeto inteiro.

## Como rodar

```bash
cd airflow_lappis/dags/dbt/mcid
export MINIO_ENDPOINT=... MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... MINIO_BUCKET=...
dbt run --target staging_duckdb --select historico_mcmv_empreendimentos_snapshot+
```

## Observacao

Nao incluir credenciais MinIO em commit. Usar `.env`.
