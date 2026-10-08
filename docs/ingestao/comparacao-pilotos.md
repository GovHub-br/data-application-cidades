# Comparação dos pilotos: bronze antigo × novo (Fase 5)

Cada DAG piloto rodou contra a fonte real gravando só em `tests/` do MinIO de dev
(`INGESTION_STORAGE_PREFIX=tests/`). A comparação é feita com DuckDB sobre o
MinIO. De um lado fica a staging antiga (`staging/<fonte>/<dado>.parquet`, que é o
bronze atual). Do outro fica o `latest/` novo, já com o renome e o filtro da prata.
Conferimos as linhas, as colunas e os valores, em texto e tipados com o mesmo cast
da prata.

| DAG | Situação |
|---|---|
| `incc_m_ingest_dag` | validada em 08/10/2026 |
| `bacen_sgs_ingest_dag` | pendente: depende da Variable `BACEN_SERIES` |
| `dotacao_execucao_outras_fontes_mcid_ingest_dag` | pendente: depende da Variable `email_credentials` e do anexo real |

## INCC-M (`fgv/incc_m`)

Execução: `tests/raw/fgv/incc_m/2026-10-08/164900/` (xlsx + `_SUCCESS`) →
`tests/staging/fgv/incc_m/2026-10-08/164900/` e `tests/staging/fgv/incc_m/latest/`.

| | Antigo | Novo (staging crua) | Novo (filtro da prata) |
|---|---|---|---|
| Linhas | 383 | 414 | 384 |
| Período | 1994-09 a 2026-07 | — | 1994-08 a 2026-07 |
| Colunas | `mes`, `indice`, `var_*`, `dt_ingest` | `column_1`, `column_2`, `No mês`, `No ano`, `12 meses`, `column_6`…`column_13` | as cinco da prata |

**Diferenças, todas explicadas:**

1. **A linha de 1994-08 (índice 100, mês-base) agora entra.** O código antigo lia
   com `skiprows=3` e `names=[...]` ao mesmo tempo. Com isso, o pandas consumia a
   primeira linha de dados como cabeçalho e a descartava. É uma correção.
   - Efeito no ouro: `ouro_conjuntura_incc_m` usa `lag(indice, 12)`, então 1995-08
     passa a ter o índice de doze meses antes, que antes era nulo.
   - Os meses do boletim não mudam.
2. **Há 30 linhas a mais na staging crua:** 29 linhas vazias no fim da planilha e
   o rodapé `Fonte: FGV`. A prata já descarta as duas coisas
   (`column_1 is not null and column_1 not ilike 'fonte%'`).
3. **As colunas `column_6` a `column_13` vêm de um quadro auxiliar à direita da
   série.** Elas trazem variações de 2021 em diante, com 80 linhas preenchidas. O
   código antigo lia só `A:E`. A staging guarda tudo como veio e a prata seleciona
   as cinco colunas da série.
4. **Inteiros aparecem sem casa decimal:** `2` no novo e `2.0` no antigo, em
   2001-05 e 2021-03. O openpyxl entrega o valor da célula e o pandas passava por
   float. Depois do `::numeric` da prata, os valores são iguais.
5. **`dt_ingest` não existe mais como coluna:** a data da ingestão sai da partição
   no `filename`, conforme decidido na Fase 0.

**Valores tipados** (cast da prata nos dois lados): as 383 linhas antigas estão
todas no novo com valores idênticos. A única linha a mais é 1994-08.
