# AGENTS.md

Repo-specific guidance for OpenCode agents. Compact and high-signal only.

## Project

Pipeline to classify and treat historical MCMV (Minha Casa Minha Vida) data from a PostgreSQL dump, as the base for an ML model predicting construction paralysations. Scope: stages 1–4 (inventory → classify → treat → enrich). Extraction / DAG / dbt / predictive model are out of scope (future feature #53).

**Status:** Stages 1+3 (classification + treatment) are implemented and tested. `main.py` runs the full pipeline: classify → compare reference → write reports → dedup MD5 hashes → treat canonical tables → quality report. Stage 4 (enrichment metadata) is partially done within treatment. Stage 2 (inventory phase) is implicit in classification output.

**Active OpenSpec changes:** `tratar-tabelas-pipe-single-col`, `corrigir-pipeline-db`, `limpeza-dados-tratamento` (in `openspec/changes/`). Check these before working on DB pipeline, pipe-delimited table treatment, or data cleaning.

## Toolchain

- uv-managed Python 3.11+ (`.python-version` says `3.11`). Run everything through `uv run`.
- Install: `uv sync`
- Run pipeline: `uv run python main.py` (add `--inventario` to enable Stage 4: inventory + quality report + timeline chart). With `--db`: runs against PostgreSQL instead of CSV samples.
- Lint/format: `uv run ruff check .` / `uv run ruff format .` (default config, no `[tool.ruff]` in pyproject.toml)
- Typecheck: `uv run mypy .`
- Tests: `uv run pytest` (runs all tests in `tests/`; `pythonpath = ["src"]` in pyproject.toml)
- `--skip-classify`: Pula classificação e carrega de fonte existente (CSV ou PG). Exemplo: `uv run python main.py --skip-classify`
- `--classify-only`: Executa apenas classificação, sem dedup/tratamento. Exemplo: `uv run python main.py --classify-only`
- As flags são mutuamente exclusivas. Usar ambas causa `sys.exit(2)`.
- `--classify-only` ignora `--inventario` com aviso.
- `--inventario` só funciona no modo CSV (não tem efeito com `--db`).
- Add deps: `uv add <pkg>` (runtime) or `uv add --dev <pkg>` (dev)

## Source layout

- `src/` layout. The importable package is `classificacao` (not `src.classificacao`), with modules:
  - `carregamento.py` — CSV loading (tab-separated, `index_col=0`, `dtype=str`, `on_bad_lines="skip"`)
  - `profiling.py` — structural profiling and column classification heuristics (R3a)
  - `regras.py` — decision tree R1–R8 that assigns a structural category to each table (17 categories, see below)
  - `classificador.py` — orchestration: iterates all samples, compares against manual reference, generates reports. Also: `classificar_todas_db()` for DB mode.
  - `saida.py` — writes `data/classificacao_formacao.csv`
  - `deduplicacao.py` — MD5-hash-based dedup of sample CSVs. Also: `agrupar_duplicatas_db()`, `gerar_mapping_db()` for DB mode.
  - `inferencia_colunas.py` — column name inference for headerless tables (CNPJ, codes, dates, monetary values, UH, etc.)
  - `inventario.py` — data inventory: front identification, period extraction, dimension detection, utility scoring
  - `tratamento.py` — main treatment pipeline for `bem_formada` tables (Groups 1–8: normalize columns, decimal separator, date parsing, encoding repair, type canonization, profile classification, period/institution extraction)
  - `tratamento_cabecalho.py` — treatment for displaced-header tables
  - `tratamento_especiais.py` — treatment for special categories (separador_|, sub_tabelas, compostos)
  - `tratamento_subtabelas.py` — sub-table extraction heuristics
  - `tratamento_pipe_single_col.py` — treatment for BB pipe-delimited single-column tables (DB mode only: `detectar_tabelas_pipe`, `tratar_tabela_pipe`)
  - `saida_tratamento.py` — writes treated CSVs to `data/treated_tables/` + generates `_quality_report.csv`
  - `validacao.py` — post-treatment validation (missing%, row/col counts)
  - `db/connection.py` — SQLAlchemy engine/session management, `.env` loading (`get_engine`, `get_session`)
  - `db/reader.py` — DB read: `listar_tabelas()`, `ler_tabela()`, `ler_schema_colunas()`, `calcular_hash_db()`, `ler_classificacao_db()`
  - `db/writer.py` — DB write: `criar_schema_target()`, `escrever_tabela()`, `escrever_classificacao()`, `escrever_qualidade()`, `escrever_dedup_map()`
- `main.py` at repo root is the entry point: classify → dedup → treat → quality report. With `--inventario`: also runs inventory, quality report, and timeline chart (`scripts/gerar_linha_do_tempo.py`). With `--db`: runs full pipeline against PostgreSQL (requires `.env` with valid credentials). DB access is now transparent — no VPN tunnel or network namespace required.
- O modo DB acessa o banco diretamente via `uv run python main.py --db [flags]`. O túnel OpenVPN e o proxy socat foram substituídos por acesso transparente ao banco. Basta configurar `.env` com as credenciais corretas.

## Data layout

- `data/table_samples/` — 753 CSV files (200 lines each), **tab-separated** (`sep="\t"`), first column is a row index (discard via `index_col=0`). **This directory is gitignored.**
- `data/columns_202605211425_perfil_cidades_dados_historicos.csv` — schema inventory from PostgreSQL dump.
- `data/classificacao_formacao.csv` — classification output (gitignored, generated by `main.py`).
- `data/relatorio_divergencias.md` — divergence report (gitignored, generated by `main.py`).
- `classificacao_formacao_revisado_autoritativo.md` (repo root, **not** in `data/`) — the authoritative reference for category definitions. Read by `comparar_referencia()` in `classificador.py`.
- `data/exemplos_por_categoria.md` — visual reference: 1 real CSV sample per category (16 categories).
- `data/treated_tables/` — output for stage 3 (~440 treated CSVs + `_dedup_map.csv` + `_quality_report.csv`). **Gitignored.**
- `data/inventario_dados.csv` — inventory output (generated by `--inventario`).
- `data/relatorio_qualidade_dados.md` — data quality report (generated by `--inventario`).
- `data/linha_do_tempo.png` — timeline chart (generated by `--inventario`, via `scripts/gerar_linha_do_tempo.py`).

## SFTP batimento (issue #95)

- `src/sftp/` — package with reusable modules for dump × SFTP reconciliation:
  - `leitura_artefatos.py` — CSV reading of SQL artefacts from `data/sftp/artefatos/`
  - `normalizacao.py` — table/column name normalization for fuzzy matching
  - `matching.py` — 3-layer structural matching (hash exact → canonical stem → Jaccard column similarity) and key identification
  - `relatorio.py` — markdown report generation with 5 sections (related tables, common fields, cross-reference keys, divergences, recommendations)
- `scripts/sftp/` — CLI analysis scripts:
  - `batimento_estrutura.py` — runs structural matching, outputs 4 CSVs to `data/sftp/analise/`
  - `gerar_queries_amostragem.py` — generates SQL sampling queries for DBeaver
  - `validar_amostras.py` — validates content samples extracted via DBeaver
  - `cruzamento_conteudo.py` — cross-references APF codes between dump and SFTP
  - `verificar_consistencia_temporal.py` — temporal consistency checks
- `data/sftp/artefatos/` — raw SQL exports from PostgreSQL (DBeaver)
- `data/sftp/analise/` — generated analysis CSVs (by `batimento_estrutura.py`)
- `data/sftp/relatorios/` — final markdown reports
- Tests in `tests/sftp/`: `test_chaves.py`, `test_leitura_artefatos.py`, `test_matching.py`, `test_normalizacao.py`

## Domain & classification taxonomy

- Source tables from two institutions: `bb_` (Banco do Brasil), `caixa_` (Caixa). Frentes: FAR, Rural, Entidades, FGTS, PNHR, PNHB.
- **Classification reference:** `classificacao_formacao_revisado_autoritativo.md` at repo root — the authoritative source for category definitions (~17 categories with examples). This is what `comparar_referencia()` reads.
- **The legacy file `categorias_classificacao.md` no longer exists.** The code checks `_OPERACIONAL_DEFAULT.exists()` and falls through gracefully; the reference comparison now uses only the authoritative doc.
- **Code taxonomy** (17 categories, from `regras.py`):
  `bem_formada`, `sem_cabecalho`, `cabecalho_na_primeira_linha_1`, `cabecalho_na_primeira_linha_2`, `cabecalho_na_segunda_linha`, `cabecalho_na_terceira_linha_1`, `cabecalho_na_terceira_linha_2`, `cabecalho_composto_1`, `cabecalho_composto_2`, `sub_tabelas_1`, `sub_tabelas_2`, `sub_tabelas_3`, `sub_tabelas_4`, `separador_|`, `vazia`, `dados_sem_utilidade`, `nao_colunares_tipo1`, plus `indeterminada` for load errors.
- Backward-compat aliases exist in regras.py: `cabecalho_na_primeira_linha` → `cabecalho_na_primeira_linha_1`, `cabecalho_na_terceira_linha` → `cabecalho_na_terceira_linha_1`.
- **Decision tree order** (D4, `classificar_formacao`):
  R2 (name patterns → dados_sem_utilidade) → R1 (empty) → R3 (pipe separator → separador_|) → R3b (header decision: real → R4 type consistency → bem_formada; data → R5 sub-tables check → sem_cabecalho/nao_colunares) → special "Posicao:" check → R5 (sub-tables) → R6 (compound header) → R7 (displaced header) → R8 (fallback).

## OpenSpec workflow

This repo uses OpenSpec (spec-driven dev). Config: `openspec/config.yaml`. Commands: `.opencode/commands/opsx-*.md`; skills: `.opencode/skills/openspec-*`.

- OpenSpec artifacts (proposals, specs, tasks) are written in **Brazilian Portuguese**, but technical terms stay in English; code and file paths stay in English.
- Proposals: under 1500 words, always include a "Non-goals" section.
- Tasks: break into chunks of max 2 hours.
- In-progress changes: `openspec/changes/`. Archived: `openspec/changes/archive/`. Synced specs: `openspec/specs/`.

## Gotchas

- CSV files in `data/table_samples/` are **tab-separated**, not comma-separated. Always use `sep="\t"`.
- The first column of every sample CSV is a row index — always discard with `index_col=0`.
- Generated outputs (`data/classificacao_formacao.csv`, `data/relatorio_divergencias.md`) are gitignored. Do not commit them.
- `data/treated_tables/` and `data/table_samples/` are gitignored — the pipeline re-creates them on each run.
- `categorias_classificacao.md` no longer exists; don't create it. The code handles its absence.
- `openspec/specs/` holds synced specs. Delta specs in changes are merged here via `/opsx-archive` (sync step).
- Tests use fixtures in `tests/fixtures/` (synthetic CSVs matching each category). Integration tests (`test_classificador.py`) need full `data/table_samples/` to exist.
- `uv.lock` must be committed; `.venv/` must not.
- `.env` contains database credentials — never commit it. Use `.env.example` as template.
- `pythonpath = ["src"]` in pyproject.toml is critical: without it `uv run pytest` can't find `classificacao` or `sftp` imports.
- DB mode (`--db`) reads full tables from PostgreSQL, not 200-line samples. The classification may diverge from CSV-sample results; divergences are reported in `data/relatorio_divergencias_db.md`.
- DB mode requires `.env` with valid credentials. DB access is now transparent — no VPN tunnel or network namespace needed. Run directly: `uv run python main.py --db [flags]`.
- SFTP test files live in `tests/sftp/`. This directory must NOT contain `__init__.py` — it shadows the `src/sftp/` package during pytest collection. The conftest.py handles the `src/` path insertion.
- SFTP artefatos (`data/sftp/artefatos/`) are extracted manually via DBeaver SQL queries, not by the Python pipeline. The scripts only read/generate from them — never write to this directory except `gerar_queries_amostragem.py` which writes `.sql` files.
- The pipe-single-column treatment (`tratamento_pipe_single_col.py`) is only active in DB mode, not CSV mode.
- DB mode generates persistent output files in `data/` with timestamped names (`_classificacao_*_perfil_*.csv`, etc.). These are not fully gitignored — some are tracked. Use `git status` to check what's clean before committing.
- `mypy` config uses `ignore_missing_imports = true` — untyped third-party deps (pandas, etc.) won't cause errors. Internal modules are still checked.
