# Categorias de formacao MCMV

Use this reference when deciding the structural category of a table sample.

## Decision tree summary

Rules are applied in fixed order; first match wins:

1. `dados_sem_utilidade`: table name contains known non-data/log patterns such as `tab_arquivos_dados`, `loginfesta`, or `novo_relat_rio_executivo`.
2. `vazia`: file is very small and has at most one column.
3. `separador_|`: data cells contain pipe-delimited records that must be expanded.
4. `bem_formada`: headers are real descriptive column names and column types are coherent.
5. `sem_cabecalho`: pandas interpreted the first data row as header.
6. `sub_tabelas_*`: multiple independent tables/blocks appear inside one file.
7. `cabecalho_composto_*`: real header spans multiple rows from merged Excel cells.
8. `cabecalho_na_*`: real header is displaced into row 1, 2, or 3.
9. Fallback: `nao_colunares_tipo1` if sparse/non-tabular; otherwise low-confidence `sem_cabecalho`.

## The 17 categories

- `bem_formada`: all columns have descriptive names; data is already tabular.
- `sem_cabecalho`: header row is actually data; column names need inference.
- `cabecalho_na_primeira_linha_1`: first data row contains true header; no final totalization complication.
- `cabecalho_na_primeira_linha_2`: first data row contains true header and there are empty lines before final totals.
- `cabecalho_na_segunda_linha`: first row has `Posicao: DD/MM/YYYY`; second row is true header.
- `cabecalho_na_terceira_linha_1`: first two rows are sparse/empty; third row is true header.
- `cabecalho_na_terceira_linha_2`: third-row header with specific report keywords such as publico-alvo, concluida, total geral, or percentage bands.
- `cabecalho_composto_1`: three header rows, often from merged Excel cells, plus footer metadata.
- `cabecalho_composto_2`: two header rows, often from merged Excel cells.
- `sub_tabelas_1`: blocks with timestamp-like columns such as `YYYYMMDD_hhmmss` plus `unnamed_0`.
- `sub_tabelas_2`: unnamed columns and report blocks with keywords like SINTESE, Faixa, Regiao, Quadro de Valores.
- `sub_tabelas_3`: one descriptive column plus unnamed columns, with blocks separated by one empty row.
- `sub_tabelas_4`: all unnamed columns with block-specific composite headers.
- `separador_|`: pipe-separated values are embedded in cells. In human-facing reports, call this `separador pipe`; keep `separador_|` as the machine-readable category value used by the pipeline.
- `vazia`: no relevant data.
- `dados_sem_utilidade`: metadata, log, or operational tables not useful for MCMV predictive analysis.
- `nao_colunares_tipo1`: sparse/report-like content that is not reliably tabular.

## Common review traps

- Do not call a table `bem_formada` just because pandas created column names; verify they are semantic names, not data values.
- Do not call a table `sem_cabecalho` before checking for sub-tables when many headers look like data values.
- Distinguish `cabecalho_composto_*` from `sub_tabelas_*`: composite headers describe one table; sub-tables contain multiple independent blocks.
- Treat low-confidence fallback categories as review targets, not final truth.
- A table can be structurally valid but still questionable for ML if keys, period, or source context are missing.
