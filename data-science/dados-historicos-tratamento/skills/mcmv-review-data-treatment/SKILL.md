---
name: mcmv-review-data-treatment
description: Review MCMV historical data treatment quality for predictive analytics and issue work. Use when Codex needs to inspect treated MCMV tables row by row with pandas, validate cleaning, field standardization, dates, monetary values, duplicate handling, encoding, inferred types, discarded/error tables, SFTP/MinIO treatment impacts, identify missing or excessive transformations, document adjustments, or generate official evidence artifacts and a polished HTML/PDF report using React/Twind-ready frontend output for data-application-cidades / dados-historicos-tratamento.
---

# MCMV Data Treatment Review

## Overview

Use this skill to audit the MCMV historical data treatment pipeline with evidence. The goal is to verify whether each treated table is clean, standardized, typed, deduplicated, encoded correctly, and usable for future predictive analytics and strategic alerts.

Prefer direct pandas inspection of raw samples and treated CSVs. Existing quality reports, inventory snapshots, dedup maps, and SFTP diffs are evidence inputs, not substitutes for looking at the data when files are available.

## Locate The Project

Work from the repository or subdirectory containing:

- `main.py`
- `src/classificacao/tratamento.py`
- `src/classificacao/tratamento_especiais.py`
- `src/classificacao/tratamento_cabecalho.py`
- `src/classificacao/tratamento_subtabelas.py`
- `src/classificacao/deduplicacao.py`
- `src/classificacao/validacao.py`
- `data/treated_tables/` or `_qualidade_*.csv`

Read `references/tratamento-pipeline.md` before reviewing implementation details. Read `references/quality-checks.md` before classifying a finding as missing, excessive, or acceptable.

## Core Workflow

1. Build context:
   - Read `docs/guia-revisao-tratamento.md` when present.
   - Read latest `_qualidade_*.csv`, `_dedup_map_*.csv`, `_classificacao_*.csv`, `data/inventario_dados.csv`, and `data/columns_*.csv`.
   - Read treatment modules only as needed to understand a disputed behavior.

2. Regenerate treatment when possible:
   - Existing classification: `uv run python main.py --skip-classify`
   - Full pipeline: `uv run python main.py`
   - Focused tests: `uv run pytest tests/ -k tratamento` and `uv run pytest tests/ -k validacao`
   - If dependencies, DB, or samples are unavailable, continue from existing artifacts and state the limitation.

3. Generate evidence with pandas:
   - Prefer `scripts/audit_treatment_quality.py` from this skill.
   - Use `pd.read_csv(..., sep="\t", dtype=str, keep_default_na=False)` for treated tables.
   - For suspicious files, also inspect raw text and try delimiter inference.
   - Save CSV evidence, compact sample prints, and summary metrics.

4. Review table by table:
   - First: `status=error`, `status=discarded`, zero shape, high `missing_pct`, encoding/date/type warnings.
   - Second: tables affected by SFTP/MinIO path, filename, zip expansion, duplicates, or bad zip.
   - Third: all treated tables when the user asks for maximum rigor.
   - Compare raw sample versus treated output when both exist; otherwise compare quality, inventory, dedup, and treated CSV.

5. Decide findings:
   - **Treatment missing:** dirty value, bad date, duplicated row, mojibake, unnormalized field, wrong type, or unparsed value remains after treatment.
   - **Treatment excessive:** useful rows/columns were dropped, identifiers lost leading zeros, dates/values were coerced to null incorrectly, or aggregation removed predictive granularity.
   - **Treatment acceptable:** transformation is supported by raw structure, quality metrics, and future analytical use.
   - **Uncertain:** raw sample or SFTP diff is missing, or evidence conflicts.

6. Produce deliverables:
   - Reviewed treatment list.
   - Quality problems found.
   - Needed adjustments and recommended improvements.
   - Doubtful/uncertain tables.
   - SFTP/MinIO impact notes.
   - Official HTML/PDF report and issue summary.

## Evidence Requirements

For each reviewed table capture, when available:

- Source table name, classification, treatment status, profile, institution, report date.
- Raw and treated shape.
- Missing percentage before/after or treated-only when raw is absent.
- Column normalization evidence: original names, final names, duplicate/empty/unnamed columns.
- Date parse evidence: candidate columns, parse success rate, min/max date, invalid examples.
- Numeric/value evidence: candidate value columns, parse success rate, invalid examples, negative/outlier flags.
- Identifier evidence: APF/CNPJ/contrato/cod/nr/null rates, leading-zero risk.
- Encoding evidence: mojibake/replacement-character counts and examples.
- Duplicate evidence: dedup map group, content hash, duplicate row percentage, duplicate column names.
- Sample prints from pandas for corrected, problematic, or uncertain tables.

## SFTP Impact Check

Check local SFTP diff artifacts when present:

- `diff_sftp_minio_path_mappings.json`
- `diff_sftp_minio_expand_zips.json`
- `diff_sftp_minio_expand_zips_with_badzip.json`
- `diff_sftp_minio_filename.json`
- `diff_sftp_minio.json`

Flag treatment impact when source path, filename, zip expansion, missing files, duplicate files, or bad zip changes the table identity, period extraction, institution inference, content hash, dedup canonical table, or treatment status.

## Official Report

When the user asks for evidence, official document, polished PDF, HTML/CSS/React/Twind, or issue-ready output:

1. Read `references/react-twind-report.md`.
2. Run the audit script and render script, adjusting output names to the issue when needed.
3. Use a polished static HTML fallback for PDF reliability, with React/Twind-ready assets or hydrated UI when dependencies/CDNs are available.
4. Include tables, counts, charts, pandas sample prints, and explicit limitations.

Default output names:

```text
docs/evidencias/revisao-tratamento-dados/
docs/revisao-tratamento-dados.html
docs/revisao-tratamento-dados.pdf
docs/resposta-revisao-tratamento-dados.md
```

## Scripts

Use the bundled scripts from this skill when practical:

```bash
python scripts/audit_treatment_quality.py --root <dados-historicos-tratamento>
python scripts/render_treatment_review_report.py --root <dados-historicos-tratamento>
```

If running from the repository copy of the skill:

```bash
python data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment/scripts/audit_treatment_quality.py --root data-science/dados-historicos-tratamento
python data-science/dados-historicos-tratamento/skills/mcmv-review-data-treatment/scripts/render_treatment_review_report.py --root data-science/dados-historicos-tratamento
```

## Decision Rules

- Do not claim row-by-row raw validation when `data/table_samples/` or treated CSVs are absent; say which evidence was available.
- Keep identifiers as strings unless evidence proves they are measures.
- Prefer `separador pipe` in prose; keep `separador_|` only for category values.
- Treat high missingness as a review flag, not automatically as an error.
- Treat dedup hashes as content evidence; do not call files duplicate by name alone.
- For predictive MCMV work, flag tables without stable key, period, institution, lineage, or event granularity even when technically treated.
