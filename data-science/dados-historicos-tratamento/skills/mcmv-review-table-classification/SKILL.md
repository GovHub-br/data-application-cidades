---
name: mcmv-review-table-classification
description: Review and correct structural classifications of MCMV historical tables for issue #96 and related predictive analytics work. Use when Codex needs to inspect MCMV table samples row by row with pandas, validate automatic versus manual classifications, choose the most coherent structural category from evidence, generate sample prints/tables/figures, identify misclassified or unclear tables, assess SFTP/MinIO path impacts, update classification artifacts, or produce an official-style Ministry of Cities / MCMV evidence report as Markdown, HTML, and PDF for data-application-cidades / dados-historicos-tratamento.
---

# MCMV Table Classification Review

## Overview

Use this skill to execute issue #96: review the structural classification of MCMV historical tables by inspecting the actual samples with pandas, compare automatic and manual classifications, choose the most coherent category from evidence, correct wrong labels when evidence is clear, mark uncertain cases, and produce an official-style evidence report.

Prefer direct evidence from `data/table_samples/`. Generated artifacts and manual references are inputs to review, not substitutes for inspecting the data. Do not invent classifications when samples or reference files are missing.

## Locate The Project

Work from the repository or subdirectory that contains the historical treatment pipeline. Look for these files first:

- `main.py`
- `src/classificacao/regras.py`
- `src/classificacao/classificador.py`
- `classificacao_formacao_revisado_autoritativo.md`
- `data/table_samples/`
- `data/classificacao_formacao.csv`
- `data/relatorio_divergencias.md`

If the current checkout does not contain them, search nearby workspace roots. If still absent, report that the historical treatment project or generated artifacts are missing and list what is needed.

## Core Workflow

1. Build context:
   - Read `classificacao_formacao_revisado_autoritativo.md` if present.
   - Read `data/relatorio_divergencias.md` and `data/classificacao_formacao.csv` if generated.
   - Read `src/classificacao/regras.py` and `src/classificacao/profiling.py` only as needed to understand a disputed decision.
   - Read `references/categorias-formacao.md` for the category taxonomy when category boundaries are unclear.

2. Regenerate classification when the project is runnable:
   - CSV mode: `uv run python main.py --classify-only`
   - DB mode, only when credentials/environment are already available or the user asks for DB mode: `uv run python main.py --db --classify-only`
   - If dependency or network setup is missing, continue from existing generated files and state the limitation.

3. Prioritize review:
   - First: rows in `relatorio_divergencias.md`.
   - Second: `confidence=low` in `classificacao_formacao.csv`.
   - Third: categories prone to false positives: `sub_tabelas_*`, `cabecalho_composto_*`, `sem_cabecalho`, `nao_colunares_tipo1`.
   - Fourth: tables affected by SFTP/MinIO differences.
   - If the user asks for maximum coherence, inspect every table with a classification row, not only divergences.

4. Inspect each table with pandas:
   - Open the corresponding sample in `data/table_samples/`.
   - Use `pd.read_csv(path, sep="\t", index_col=0, dtype=str, keep_default_na=False)` for the main inspection.
   - Also inspect raw text for delimiter/path/encoding surprises when pandas output is suspicious.
   - Capture evidence for each reviewed table: shape, columns, first 5-10 rows, empty-row ratio, unnamed-column ratio, pipe-cell count, header-like rows, and table-block separators.
   - Compare actual structure with the 17 category definitions.
   - Decide whether the problem is the automatic rule, the manual reference, the sample/path mapping, or an unclear table.
   - Save compact sample prints in the review report. For large batches, include full prints for corrected/uncertain tables and summarized metrics for unchanged tables.

5. Apply corrections conservatively:
   - If the manual reference is clearly wrong, update `classificacao_formacao_revisado_autoritativo.md`.
   - If the rule is clearly wrong across a pattern, update `src/classificacao/regras.py` or supporting profiling code and add/adjust focused tests.
   - If only one table is ambiguous, document it as uncertain instead of forcing a rule change.
   - Preserve unrelated user changes.

6. Verify:
   - Re-run `uv run python main.py --classify-only` when possible.
   - Run focused tests such as `uv run pytest tests/ -k classificador` when present.
   - Confirm divergences decreased or explain remaining justified divergences.

## SFTP Impact Check

Issue #96 asks to review the impact of SFTP on existing classifications. Check local SFTP diff artifacts when present:

- `diff_sftp_minio_path_mappings.json`
- `diff_sftp_minio_expand_zips.json`
- `diff_sftp_minio_expand_zips_with_badzip.json`
- `diff_sftp_minio_filename.json`
- `diff_sftp_minio.json`

Look for renamed, moved, expanded-from-zip, missing, duplicated, or bad-zip tables. Note when a classification changed because the source path, filename, zip expansion, or table identity changed rather than because the structural classifier is wrong.

## Deliverables

Produce or update review artifacts using `references/issue-96-report-template.md` and `references/official-evidence-report.md`:

- Reviewed tables.
- Pandas inspection evidence and sample prints.
- Corrected tables.
- Tables with doubtful classification.
- Observed inconsistencies.
- SFTP/MinIO impact notes.
- Commands run and verification status.
- Official-style report files:
  - `docs/revisao-classificacao-issue-96.md`
  - `docs/revisao-classificacao-issue-96.html`
  - `docs/revisao-classificacao-issue-96.pdf`

When editing repository docs, prefer a focused file such as `docs/revisao-classificacao-issue-96.md` unless the project already has a better convention.

## Official Evidence Report

When the user asks for an official document, Ministry of Cities/MCMV style, evidence, prints, figures, or a ready PDF:

1. Read `references/official-evidence-report.md`.
2. Generate a polished Markdown report with:
   - Cover/title block.
   - Document metadata: project, issue, date, repository branch, responsible reviewer, data source, method.
   - Executive summary with counts and key findings.
   - Methodology using pandas, classification taxonomy, and acceptance criteria.
   - Evidence tables with before/after classification, sample shape, pipe counts, empty-row ratio, and decision.
   - Prints from pandas (`df.head().to_string()`) for corrected or uncertain tables.
   - Figures when useful: category distribution bar chart, decision flow, SFTP/MinIO impact summary.
   - Annexes: commands, limitations, hashes/dedup observations, list of reviewed tables.
3. Render Markdown to HTML and PDF with `scripts/render_official_report.py`.
4. If PDF rendering fails because a tool is missing, leave the Markdown and HTML ready and clearly report the missing PDF step.

Default output names:

```text
docs/revisao-classificacao-issue-96.md
docs/revisao-classificacao-issue-96.html
docs/revisao-classificacao-issue-96.pdf
```

## Decision Rules

- Treat `classificacao_formacao_revisado_autoritativo.md` as the manual authority, but not infallible.
- A final category must be supported by pandas inspection of the sample when samples are available.
- A category correction needs visible evidence from sample structure, sample print, table name, or source mapping.
- A rules-code change needs a recurring pattern or a high-impact false classification, not only a one-off disagreement.
- Keep uncertain cases explicit with a reason and suggested next check.
- For predictive MCMV use, flag tables that are structurally valid but analytically weak: no APF/empreendimento key, unclear period, source ambiguity, excessive sparseness, or report-style aggregation.
- Use human-readable `separador pipe` in reports and discussions. Keep `separador_|` only when writing the code/category value expected by existing CSVs and Python constants.
- Do not assume tables in the same structural category are duplicate content. Check `_dedup_map` hashes or content hashes before saying they are identical.

## Pandas Inspection Pattern

Use this pattern when reviewing one or many samples:

```python
from pathlib import Path

import pandas as pd

samples_dir = Path("data/table_samples")

def inspect_sample(table_name: str) -> dict[str, object]:
    path = samples_dir / f"{table_name}.csv"
    df = pd.read_csv(path, sep="\t", index_col=0, dtype=str, keep_default_na=False)
    cols = [str(c) for c in df.columns]
    empty_rows = df.eq("").all(axis=1).mean() if len(df) else 0
    unnamed_cols = sum(c.lower().startswith("unnamed") or c == "" for c in cols)
    pipe_cells = df.astype(str).apply(lambda s: s.str.contains(r"\|", regex=True)).sum().sum()
    return {
        "table_name": table_name,
        "shape": df.shape,
        "columns": cols[:12],
        "empty_row_ratio": round(float(empty_rows), 4),
        "unnamed_col_ratio": round(unnamed_cols / max(len(cols), 1), 4),
        "pipe_cells": int(pipe_cells),
        "head": df.head(8).to_string(),
    }
```

For `separador pipe`, verify at least one data column has pipe-delimited values and show a short before/after expansion when useful:

```python
first_col = df.columns[0]
expanded = df[first_col].str.split("|", expand=True)
print(df[[first_col]].head(5).to_string())
print(expanded.head(5).to_string())
```
