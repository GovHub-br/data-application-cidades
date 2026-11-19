# Official evidence report

Use this reference when the user wants an official-looking document for the Ministry of Cities / MCMV front, with evidence, tables, prints, figures, and a PDF deliverable.

## Layout target

Use an A4 document style inspired by the Gov Hub official data document:

- Cover/title block:
  - `Ministério das Cidades`
  - `Minha Casa, Minha Vida`
  - `Frente Cidades - Dados Historicos`
  - Report title, e.g. `Revisao de Classificacao das Tabelas Historicas`
  - Subtitle, e.g. `Evidencias, divergencias e correcoes para a issue #96`
  - Date and repository/branch.
- Sumario / table of contents.
- Numbered sections:
  1. Visao Geral
  2. Metodologia de Revisao
  3. Resultado Consolidado
  4. Evidencias por Divergencia
  5. Impacto SFTP/MinIO
  6. Limitacoes e Proximos Passos
  7. Anexos
- Tables should be dense and audit-friendly.
- Prints should be short, readable, and captioned.
- Figures should have clear titles and captions.

## Required evidence

Every corrected or doubtful table needs at least:

- Current classification and revised classification.
- Decision: `mantida`, `corrigida`, or `duvidosa`.
- Evidence source: sample path, generated artifact, reference doc, or inventory.
- Pandas metrics:
  - `shape`
  - `columns_preview`
  - `empty_row_ratio`
  - `unnamed_col_ratio`
  - `pipe_cells`
  - `head_print`
- For `separador pipe`, include a print showing values containing `|`.
- If claiming tables are duplicates or identical, include hash evidence from `_dedup_map`; otherwise say only that they share a structural category.

## Recommended figures

Create figures only when they add evidence:

- Category distribution after review.
- Before/after count of low-confidence divergences.
- Bar chart of corrected tables by category.
- SFTP/MinIO impact summary, such as added/removed/modified counts by diff artifact.

If `matplotlib` is available, save figures under `docs/assets/revisao-classificacao-issue-96/`.
Prefer PNG for figures that must appear in the PDF. SVG is fine for HTML, but Pandoc/LaTeX may require `rsvg-convert` to embed SVG in PDF.

## Report language

- Write in Brazilian Portuguese.
- Use a formal but direct data-audit tone.
- Prefer `separador pipe` in prose.
- Use `separador_|` only when referring to the exact category value stored in CSV/code.
- State limitations explicitly. Do not hide missing samples, missing `uv`, or unavailable DB access.

## Suggested structure

```markdown
# Revisao de Classificacao das Tabelas Historicas MCMV

**Ministerio das Cidades**  
**Minha Casa, Minha Vida - Frente Cidades**  
**Documento de evidencias de dados**

| Campo | Valor |
|---|---|
| Issue | #96 |
| Repositorio/branch | ... |
| Data da revisao | ... |
| Fonte dos dados | ... |
| Metodo | pandas + comparacao contra gabarito autoritativo |

# Sumario Executivo

Tabela com total revisado, corrigido, duvidoso, divergencias restantes.

# 1. Visao Geral

Contexto e objetivo.

# 2. Metodologia

Como as amostras foram abertas com pandas, criterios de classificacao, evidencias coletadas.

# 3. Resultado Consolidado

Distribuicao por categoria e tabela before/after.

# 4. Evidencias

Uma subseção por tabela corrigida ou duvidosa, com metricas e print.

# 5. Impacto SFTP/MinIO

Tabela de diffs e interpretacao.

# 6. Limitacoes

Ambiente, dados ausentes, comandos nao executados.

# 7. Anexos

Comandos, snippets pandas, lista de arquivos alterados.
```

## Rendering

Use `scripts/render_official_report.py` after creating the Markdown:

```bash
python3 /path/to/skill/scripts/render_official_report.py docs/revisao-classificacao-issue-96.md --output-dir docs --basename revisao-classificacao-issue-96
```

The script creates:

- `docs/revisao-classificacao-issue-96.html`
- `docs/revisao-classificacao-issue-96.pdf` when Chrome/Pandoc rendering succeeds
